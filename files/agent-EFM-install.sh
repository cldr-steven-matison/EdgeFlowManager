kubectl exec -it ssb-postgresql-68d79f94b7-jv265 -n cld-streaming -- psql -U postgres -c "CREATE DATABASE efm;"
kubectl exec -it ssb-postgresql-68d79f94b7-jv265 -n cld-streaming -- psql -U postgres -c "CREATE USER efm WITH PASSWORD 'efm_password';"
kubectl exec -it ssb-postgresql-68d79f94b7-jv265 -n cld-streaming -- psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE efm TO efm;"
kubectl exec -it ssb-postgresql-68d79f94b7-jv265 -n cld-streaming -- psql -U postgres -c "ALTER DATABASE efm OWNER TO efm;"

kubectl create secret generic efm-db-pass \
  --from-literal=password=efm_password \
  --namespace cld-streaming

eval $(minikube docker-env)
docker login container.repo.cloudera.com
docker pull container.repo.cloudera.com/cloudera/efm:2.3.1.0-2

kubectl apply -f efm-pvc.yaml
kubectl apply -f efm-deployment.yaml

#binaries need to generate windows,linx x86, linux aarch64, java
# see efm-binaries.md