BINARY_NAME=cargo_avto_update

run:
	go run app/cmd/main.go

git:
	git add .
	git commit -a -m "$m"
	git push -u origin main

build:
	rm -f ${BINARY_NAME}
	GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o ${BINARY_NAME} app/cmd/main.go
	echo "Built ${BINARY_NAME}"

test:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

# pg_dump -h localhost -U postgres -Fc mystats_db > mystats_db_23092023.sql
# pg_restore -h localhost -U postgres -d mystats_db mystats_db_23092023.sql
#
# pg_dump -h localhost -U postgres -t "public.api_stock" "mystats_db" | psql -U postgres -d "public.stock" "new_db"
