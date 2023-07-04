# article-webservice

sam build && sam local start-api --container-host localhost --skip-pull-image 

pip3 install --target . -r requirements.txt 

rm -rf __pycache__ 

zip -r ArticleFinderAPIWithFilters.zip *
