BINARY_NAME=cargo_avto_update

git:
	git add .
	git commit -a -m "$m"
	git push -u origin main