https://github.com/kubeflow/spark-operator

helm install teehr-spark-release spark-operator/spark-operator --namespace teehr-spark-default --set webhook.enable=true --create-namespace

aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin 935462133478.dkr.ecr.us-east-2.amazonaws.com

cd workflows
docker build . -t 935462133478.dkr.ecr.us-east-2.amazonaws.com/teehr-spark/spark-workflows:v20240502
docker push 935462133478.dkr.ecr.us-east-2.amazonaws.com/teehr-spark/spark-workflows:v20240502
cd ..


kubectl delete -f workflows/teehr-query.yaml 
kubectl apply -f workflows/teehr-query.yaml 

kubectl delete -f workflows/spark-pi.yaml 
kubectl apply -f workflows/spark-pi.yaml 

kubectl apply -f workflows/role.yaml
kubectl delete -f workflows/role.yaml
