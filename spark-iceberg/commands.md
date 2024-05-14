
docker build . -f Dockerfile.iceberg -t localhost:5001/teehr-spark/iceberg-spark:3.5.1-scala2.12-java17-python3-r-ubuntu
docker push localhost:5001/teehr-spark/iceberg-spark:3.5.1-scala2.12-java17-python3-r-ubuntu

docker build . -f Dockerfile.jupyter -t localhost:5001/teehr-spark/jupyter-spark:3.5.1-scala2.12-java17-python3-r-ubuntu
docker push localhost:5001/teehr-spark/jupyter-spark:3.5.1-scala2.12-java17-python3-r-ubuntu

kubectl delete -n spark -f manifests/spark-jupyter.yaml
kubectl apply -n spark -f manifests/spark-jupyter.yaml


kubectl create clusterrolebinding jupyter --clusterrole=edit --serviceaccount=spark:jupyter --namespace=spark

