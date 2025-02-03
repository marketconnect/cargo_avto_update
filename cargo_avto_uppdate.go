package cargo_avto_update

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/chromedp/chromedp"
	"golang.org/x/exp/rand"
	_ "modernc.org/sqlite"
)

const baseURL = "https://sp.cargo-avto.ru/catalog/"

// Config содержит параметры для обработки.
type Config struct {
	ObjectIDs         []int   // SubjectIDs
	DesiredMargin     float64 // DesiredMargin (for example, 0.3)
	TaxRate           float64 // TaxRate (for example, 0.07)
	Delivery          int     // Delivery (for example, 100)
	PVZ               int     // PVZ (for example, 15)
	DBName            string  // DBName (for example, "ue.db")
	VendorCodePattern string  // VendorCodePattern (for example, "^box_\d+_\d+$")
	UsePcs            bool    // UsePcs (for example, true)
}

func Process(apiKey string, cfg Config) error {
	// 1. Получаем тарифы FBS
	base, liter, err := getFBSTariffs(apiKey)
	if err != nil {
		return fmt.Errorf("ошибка получения тарифов: %v", err)
	}
	log.Printf("Тарифы FBS: base=%.2f, liter=%.2f", base, liter)

	// 2. Удаляем старую базу данных, если существует, и открываем новую
	if err := os.Remove(cfg.DBName); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("ошибка удаления старой базы данных: %v", err)
	}
	log.Println("Старая база данных удалена.")

	db, err := sql.Open("sqlite", cfg.DBName)
	if err != nil {
		return fmt.Errorf("ошибка при открытии базы данных: %v", err)
	}
	defer db.Close()

	createTable(db)

	// 3. Загружаем карточки, используя переданные objectIDs
	allCards := fetchAllCards(apiKey, cfg.ObjectIDs)
	log.Printf("Всего загружено %d карточек.", len(allCards))

	// 4. Настраиваем Chromedp для парсинга страниц
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.Flag("headless", false),
		chromedp.Flag("disable-gpu", true),
	)
	allocCtx, allocCancel := chromedp.NewExecAllocator(context.Background(), opts...)
	defer allocCancel()

	ctx, ctxCancel := chromedp.NewContext(allocCtx)
	defer ctxCancel()

	// 5. Загружаем цены товаров
	prices, err := getProductPrices(apiKey, 1000, 0, 0)
	if err != nil {
		log.Printf("Ошибка получения цен: %v", err)
	}

	// 6. Загружаем комиссии
	commissions, err := getCommission(apiKey)
	if err != nil {
		log.Printf("Ошибка получения комиссии: %v", err)
	}

	// Ищем комиссию для subjectID=3979 (пример)
	var commissionRate int
	for _, c := range commissions {
		if c.SubjectID == 3979 {
			commissionRate = int(c.KgvpMarketplace)
		}
	}
	log.Println("Комиссия:", commissionRate)

	productDataCache := make(map[string]map[string]string)
	skuMap := extractSKUs(allCards)
	vendorCodePattern := regexp.MustCompile(cfg.VendorCodePattern)
	// 7. Обрабатываем каждую карточку
	for _, card := range allCards {
		if !vendorCodePattern.MatchString(card.VendorCode) {
			log.Printf("Пропускаем товар с некорректным VendorCode: %s", card.VendorCode)
			continue
		}
		skus := skuMap[card.NmID]
		if len(skus) != 1 {
			panic(fmt.Sprintf("SKU либо отсутствует, либо их больше 1 для товара с VendorCode: %s", card.VendorCode))
		}

		// Извлекаем productID и pcs из vendorCode
		parts := strings.Split(card.VendorCode, "_")
		if len(parts) < 2 {
			log.Printf("Некорректный VendorCode: %s", card.VendorCode)
			continue
		}
		productID := parts[1]
		pcsInt := 1
		if len(parts) > 2 && cfg.UsePcs {
			if val, err := strconv.Atoi(parts[2]); err == nil {
				pcsInt = val
			}
		}

		var (
			wbPrice           float64
			wbDiscountedPrice float64
			wbClubDiscounted  float64
		)
		for _, p := range prices {
			if p.VendorCode == card.VendorCode {
				if len(p.Sizes) > 0 {
					wbPrice = p.Sizes[0].Price
					wbDiscountedPrice = p.Sizes[0].DiscountedPrice
					wbClubDiscounted = p.Sizes[0].ClubDiscountedPrice
				}
				break
			}
		}

		// Парсинг данных товара (с кешированием)
		var productData map[string]string
		if cachedData, exists := productDataCache[productID]; exists {
			log.Printf("Используем кешированные данные для товара: %s", productID)
			productData = cachedData
		} else {
			log.Printf("Парсим страницу для товара: %s", productID)
			url := baseURL + productID + "/"
			productData, err = scrapeProductData(ctx, url)
			if err != nil {
				log.Printf("Ошибка при обработке товара %s: %v", productID, err)
				continue
			}
			productDataCache[productID] = productData
		}

		// Рассчитываем стоимость с учетом количества pcs
		cost, err := convertAndMultiply(productData["price"], fmt.Sprintf("%d", pcsInt))
		if err != nil {
			log.Printf("Ошибка при конвертации и умножении для %s: %v", productID, err)
			continue
		}

		// Рассчитываем тариф
		volumeInLiters := CalculateVolumeLiters(card.Dimensions.Width, card.Dimensions.Height, card.Dimensions.Length)
		tariff := CalculateTariff(volumeInLiters, base, liter)
		fmt.Printf("volumeInLiters: %f, base: %f, liter: %f, tariff: %f\n", volumeInLiters, base, liter, tariff)

		// Рассчитываем комиссию (используем clubDiscountPrice)
		returns := (tariff + 50) / 9
		fixedCosts := cost + int(math.Ceil(tariff)) + cfg.Delivery + cfg.PVZ + int(math.Ceil(returns))
		fmt.Printf("fixedCosts: %d (cost: %d, tariff: %f, delivery: %d, pvz: %d, returns: %f)\n", fixedCosts, cost, tariff, cfg.Delivery, cfg.PVZ, returns)
		comNum := (float64(commissionRate) + 1) / 100
		okPrice, err := CalcPrice(cfg.DesiredMargin, cfg.TaxRate, comNum, float64(fixedCosts))
		if err != nil {
			log.Printf("Ошибка при расчете цены: %v", err)
			continue
		}
		commission := int(okPrice * comNum)

		// Сохраняем данные в базу
		saveToDatabase(db, SaveParams{
			NmID:              card.NmID,
			VendorCode:        card.VendorCode,
			Width:             card.Dimensions.Width,
			Height:            card.Dimensions.Height,
			Length:            card.Dimensions.Length,
			Pcs:               pcsInt,
			ProductID:         productID,
			WbPrice:           wbPrice,
			WbDiscountedPrice: wbDiscountedPrice,
			WbClubDiscounted:  wbClubDiscounted,
			AvailableCountStr: productData["availableCount"],
			Cost:              cost,
			Tariff:            tariff,
			Commission:        commission,
			OKPrice:           okPrice,
		}, skus[0])
	}

	log.Println("Обработка завершена.")
	return nil
}

// ----------------------- Вспомогательные типы и функции -----------------------

type TariffResponse struct {
	Response struct {
		Data struct {
			WarehouseList []struct {
				WarehouseName    string          `json:"warehouseName"`
				BoxDeliveryBase  json.RawMessage `json:"boxDeliveryBase"`
				BoxDeliveryLiter json.RawMessage `json:"boxDeliveryLiter"`
			} `json:"warehouseList"`
		} `json:"data"`
	} `json:"response"`
}

func parseFloat(raw json.RawMessage) (float64, error) {
	var num float64
	if err := json.Unmarshal(raw, &num); err == nil {
		return num, nil
	}
	var str string
	if err := json.Unmarshal(raw, &str); err == nil {
		str = strings.ReplaceAll(str, ",", ".")
		return strconv.ParseFloat(str, 64)
	}
	return 0, fmt.Errorf("не удалось преобразовать значение в float64")
}

func getFBSTariffs(apiKey string) (float64, float64, error) {
	url := "https://common-api.wildberries.ru/api/v1/tariffs/box"
	client := &http.Client{Timeout: 10 * time.Second}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("Authorization", apiKey)

	q := req.URL.Query()
	q.Add("date", "2025-02-01")
	req.URL.RawQuery = q.Encode()

	resp, err := client.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, 0, err
	}

	var data TariffResponse
	if err := json.Unmarshal(body, &data); err != nil {
		return 0, 0, err
	}

	for _, warehouse := range data.Response.Data.WarehouseList {
		if warehouse.WarehouseName == "Маркетплейс" {
			base, err1 := parseFloat(warehouse.BoxDeliveryBase)
			liter, err2 := parseFloat(warehouse.BoxDeliveryLiter)
			if err1 != nil || err2 != nil {
				return 0, 0, fmt.Errorf("ошибка конвертации тарифов: %v, %v", err1, err2)
			}
			return base, liter, nil
		}
	}

	return 0, 0, fmt.Errorf("не найден склад 'Маркетплейс'")
}

type CardsListResponse struct {
	Cards  []Card `json:"cards"`
	Cursor struct {
		UpdatedAt string `json:"updatedAt"`
		NmID      int    `json:"nmID"`
		Total     int    `json:"total"`
	} `json:"cursor"`
}

type Card struct {
	NmID       int           `json:"nmID"`
	VendorCode string        `json:"vendorCode"`
	Title      string        `json:"title"`
	UpdatedAt  string        `json:"updatedAt"`
	Dimensions Dimensions    `json:"dimensions"`
	Sizes      []ProductSize `json:"sizes"`
}

type ProductSize struct {
	SKUs []string `json:"skus"`
}

type Dimensions struct {
	Width   int  `json:"width"`
	Height  int  `json:"height"`
	Length  int  `json:"length"`
	IsValid bool `json:"isValid"`
}

type Size struct {
	SizeID              int64   `json:"sizeID"`
	Price               float64 `json:"price"`
	DiscountedPrice     float64 `json:"discountedPrice"`
	ClubDiscountedPrice float64 `json:"clubDiscountedPrice"`
	TechSizeName        string  `json:"techSizeName"`
}

type Product struct {
	NmID              int64  `json:"nmID"`
	VendorCode        string `json:"vendorCode"`
	Sizes             []Size `json:"sizes"`
	CurrencyIsoCode   string `json:"currencyIsoCode4217"`
	Discount          int    `json:"discount"`
	ClubDiscount      int    `json:"clubDiscount"`
	EditableSizePrice bool   `json:"editableSizePrice"`
}

type Data struct {
	ListGoods []Product `json:"listGoods"`
}

type ProductResponse struct {
	Data Data `json:"data"`
}

func getProductPrices(apiKey string, limit, offset int, filterNmID int64) ([]Product, error) {
	url := fmt.Sprintf("https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter?limit=%d&offset=%d", limit, offset)
	if filterNmID > 0 {
		url += fmt.Sprintf("&filterNmID=%d", filterNmID)
	}

	client := &http.Client{Timeout: 10 * time.Second}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", apiKey)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var response ProductResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, err
	}

	return response.Data.ListGoods, nil
}

func CalculateTariff(volumeLiters float64, boxDeliveryBase, boxDeliveryLiter float64) float64 {
	return (volumeLiters-1)*boxDeliveryLiter + boxDeliveryBase
}

func CalculateVolumeLiters(width, height, length int) float64 {
	volumeCm3 := float64(width) * float64(height) * float64(length)
	return volumeCm3 / 1000.0
}

type Commission struct {
	KgvpMarketplace     float64 `json:"kgvpMarketplace"`
	KgvpSupplier        float64 `json:"kgvpSupplier"`
	KgvpSupplierExpress float64 `json:"kgvpSupplierExpress"`
	PaidStorageKgvp     float64 `json:"paidStorageKgvp"`
	ParentID            int     `json:"parentID"`
	ParentName          string  `json:"parentName"`
	SubjectID           int     `json:"subjectID"`
	SubjectName         string  `json:"subjectName"`
}

type CommissionResponse struct {
	Report []Commission `json:"report"`
}

func getCommission(apiKey string) ([]Commission, error) {
	url := "https://common-api.wildberries.ru/api/v1/tariffs/commission"
	client := &http.Client{Timeout: 10 * time.Second}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", apiKey)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var response CommissionResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, err
	}
	return response.Report, nil
}

func getCardsList(apiKey string, updatedAt string, nmID int, objectIDs []int) (*CardsListResponse, error) {
	url := "https://content-api.wildberries.ru/content/v2/get/cards/list"
	client := &http.Client{Timeout: 10 * time.Second}

	bodyData := map[string]interface{}{
		"settings": map[string]interface{}{
			"cursor": map[string]interface{}{
				"limit": 100,
			},
			"filter": map[string]interface{}{
				"withPhoto": 1,
				"objectIDs": objectIDs,
			},
		},
	}

	if updatedAt != "" {
		bodyData["settings"].(map[string]interface{})["cursor"].(map[string]interface{})["updatedAt"] = updatedAt
	}
	if nmID != 0 {
		bodyData["settings"].(map[string]interface{})["cursor"].(map[string]interface{})["nmID"] = nmID
	}

	bodyJSON, err := json.Marshal(bodyData)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(bodyJSON))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var response CardsListResponse
	if err := json.Unmarshal(b, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func fetchAllCards(apiKey string, objectIDs []int) []Card {
	var allCards []Card
	var updatedAt string
	var nmID int

	for {
		response, err := getCardsList(apiKey, updatedAt, nmID, objectIDs)
		if err != nil {
			log.Printf("Ошибка запроса карточек: %v", err)
			break
		}
		if response == nil || len(response.Cards) == 0 {
			log.Println("Больше нет карточек для загрузки.")
			break
		}
		allCards = append(allCards, response.Cards...)
		updatedAt = response.Cursor.UpdatedAt
		nmID = response.Cursor.NmID

		if updatedAt == "" || nmID == 0 {
			break
		}
		log.Printf("Загружено %d карточек, продолжаем...", len(allCards))
	}
	return allCards
}

func scrapeProductData(ctx context.Context, url string) (map[string]string, error) {
	var productPrice string
	var availableStoresCount int

	err := chromedp.Run(ctx,
		chromedp.Navigate(url),
		chromedp.Sleep(2*time.Second),
		chromedp.Click(`li.tabs-item a[href="#samovivoz-tabs"]`, chromedp.ByQuery),
		chromedp.Sleep(2*time.Second),
		chromedp.Text(`li[data-min="1"] .price-val`, &productPrice, chromedp.ByQuery),
		chromedp.Evaluate(`document.querySelectorAll('.avail-item-status.avail').length`, &availableStoresCount),
	)
	if err != nil {
		return nil, fmt.Errorf("ошибка парсинга страницы %s: %w", url, err)
	}

	productPrice = strings.TrimSpace(productPrice)
	productPrice = strings.ReplaceAll(productPrice, "p", "")
	productPrice = strings.ReplaceAll(productPrice, " ", "")

	return map[string]string{
		"price":          productPrice,
		"availableCount": fmt.Sprintf("%d", availableStoresCount),
	}, nil
}

func convertAndMultiply(priceStr, multiplierStr string) (int, error) {
	price, err := strconv.ParseFloat(priceStr, 64)
	if err != nil {
		return 0, fmt.Errorf("ошибка преобразования price: %v", err)
	}
	roundedPrice := int(math.Ceil(price))

	multiplier, err := strconv.Atoi(multiplierStr)
	if err != nil {
		return 0, fmt.Errorf("ошибка преобразования multiplier: %v", err)
	}
	return roundedPrice * multiplier, nil
}

func createTable(db *sql.DB) {
	query := `
	CREATE TABLE IF NOT EXISTS products (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		nm_id INTEGER,
		vendor_code TEXT,
		width INTEGER,
		height INTEGER,
		length INTEGER,
		pcs INTEGER,
		product_id TEXT,
		skus TEXT,
		price REAL,
		discounted_price REAL,
		club_discounted_price REAL,
		available_count INTEGER,
		cost INTEGER,
		tariff REAL,
		commission INTEGER,
		ok_price REAL,
		new_price INTEGER,
		new_discount INTEGER,
		UNIQUE (product_id, pcs)
	);
	`
	_, err := db.Exec(query)
	if err != nil {
		log.Fatalf("Ошибка при создании таблицы: %v", err)
	}
	log.Println("Таблица products проверена/создана.")
}

// SaveParams используется для передачи параметров в функцию сохранения.
type SaveParams struct {
	NmID                  int
	VendorCode            string
	Width, Height, Length int
	Pcs                   int

	ProductID string

	WbPrice           float64
	WbDiscountedPrice float64
	WbClubDiscounted  float64

	AvailableCountStr string
	Cost              int
	Tariff            float64
	Commission        int
	OKPrice           float64
}

func saveToDatabase(db *sql.DB, params SaveParams, sku string) {
	availableCount, err := strconv.Atoi(params.AvailableCountStr)
	if err != nil {
		log.Printf("Ошибка при конвертации availableCount для %s: %v", params.ProductID, err)
		availableCount = 0
	}

	newPrice, newDiscount := calculateNewPriceAndDiscount(params.OKPrice)

	query := `
INSERT INTO products (
	nm_id, vendor_code,
	width, height, length,
	pcs, product_id, skus,
	price, discounted_price, club_discounted_price,
	available_count, cost, tariff, commission, ok_price,
	new_price, new_discount
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(product_id, pcs) DO UPDATE SET
	nm_id = excluded.nm_id,
	vendor_code = excluded.vendor_code,
	width = excluded.width,
	height = excluded.height,
	length = excluded.length,
	price = excluded.price,
	discounted_price = excluded.discounted_price,
	club_discounted_price = excluded.club_discounted_price,
	available_count = excluded.available_count,
	cost = excluded.cost,
	tariff = excluded.tariff,
	commission = excluded.commission,
	ok_price = excluded.ok_price,
	new_price = excluded.new_price,
	new_discount = excluded.new_discount,
	skus = excluded.skus;
`
	_, err = db.Exec(query,
		params.NmID, params.VendorCode,
		params.Width, params.Height, params.Length,
		params.Pcs, params.ProductID, sku,
		params.WbPrice, params.WbDiscountedPrice, params.WbClubDiscounted,
		availableCount, params.Cost, params.Tariff, params.Commission, params.OKPrice,
		newPrice, newDiscount,
	)
	if err != nil {
		log.Printf("Ошибка при сохранении данных для %s: %v", params.ProductID, err)
	} else {
		log.Printf("Данные для товара %s успешно сохранены. SKUs: %s", params.ProductID, sku)
	}
}

func CalcPrice(desiredMargin, taxRate, commissionRate, fixedCosts float64) (float64, error) {
	denominator := (1 - taxRate - commissionRate) - desiredMargin
	if denominator <= 0 {
		return 0, errors.New("невозможно достичь такой рентабельности при заданных налоге и комиссии")
	}
	price := fixedCosts / denominator
	if price < 0 {
		return 0, errors.New("получена отрицательная цена — проверьте входные данные")
	}
	fmt.Printf("desiredMargin: %f, taxRate: %f, commissionRate: %f, fixedCosts: %f\n, denominator: %f, price: %f\n",
		desiredMargin, taxRate, commissionRate, fixedCosts, denominator, price)
	return price, nil
}

func calculateNewPriceAndDiscount(okPrice float64) (int, int) {
	rand.Seed(uint64(time.Now().UnixNano()))
	markup := 1.3 + rand.Float64()*0.2 // markup в диапазоне [1.3, 1.5]
	newPrice := int(math.Round(okPrice*markup/5) * 5)
	newDiscount := int(math.Round(100 - (okPrice/float64(newPrice))*100))
	return newPrice, newDiscount
}

func extractSKUs(cards []Card) map[int][]string {
	skuMap := make(map[int][]string)
	for _, card := range cards {
		var skus []string
		for _, size := range card.Sizes {
			skus = append(skus, size.SKUs...)
		}
		skuMap[card.NmID] = skus
	}
	return skuMap
}
