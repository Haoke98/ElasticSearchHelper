mkdir -p ./rsa-license/
openssl genpkey -algorithm RSA -out ./rsa-license/elasticsearch.key -pkeyopt rsa_keygen_bits:4096
openssl req -new -key ./rsa-license/elasticsearch.key -out ./rsa-license/elasticsearch.csr -subj "/CN=ES_CLUSTER_SLRC_v8.3.3"
openssl x509 -req -in ./rsa-license/elasticsearch.csr -signkey ./rsa-license/elasticsearch.key -out ./rsa-license/elasticsearch.crt -days 3650
# 验证证书
openssl x509 -noout -text -in ./rsa-license/elasticsearch.crt



