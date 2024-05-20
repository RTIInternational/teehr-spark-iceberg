Create an EKS cluster with 2 node groups
```
cd eksctl
eksctl create cluster -f cluster.yaml 
cd ..
```
Create the JSON config per https://docs.garden.io/kubernetes-plugins/guides/in-cluster-building#using-in-cluster-building-with-ecr

```
cd eksctl
kubectl --namespace default create secret generic ecr-config \
  --from-file=.dockerconfigjson=./config.json \
  --type=kubernetes.io/dockerconfigjson
cd ..
```

## Install contour ingress controller (only needed if exposing to internet)
```bash
helm install \
  contour \
  --namespace contour \
  --create-namespace \
  --version 12.3.1 \
  oci://registry-1.docker.io/bitnamicharts/contour
```
Once ingress controller has finished and 'EXTERNAL-IP' is no longer '<pending>', set IP in DNS.
You can get the IP with the following command.
```bash
kubectl describe svc contour-envoy --namespace contour | grep Ingress | awk '{print $3}'
```

Make sure DNS changes have propagated and then move to `cert-manger`.
```bash
nslookup minio.teehr-spark.rtiamanzi.org
```

## Get SSL Certificate and configure HTTPProxy (only needed if exposing to internet)
```bash
helm install \
  cert-manager jetstack/cert-manager \
  --namespace cert-manager \
  --create-namespace \
  --version v1.12.0 \
  --set installCRDs=true

helm uninstall \
  cert-manager \
  --namespace cert-manager
```

## Autoscaler
```
helm upgrade \
  --cleanup-on-fail \
  --install teehr-ca autoscaler/cluster-autoscaler \
  --namespace teehr-spark-default \
  --set autoDiscovery.clusterName=teehr-spark-cluster \
  --set awsRegion=us-east-2
```


Need to create container repositories for each container (service), for example, `teehr-spark/spark-iceberg-image`.
Only needs to be done once.

Still have an issue with docker permissions for the service account.  
`ecr-poweruser` does not seem to work, so setting manually in AWS console for now.

Autoscaler policy needs to be added too...not sure why can't make this work.

```
garden deploy --env remote
```

```
kubectl config current-context
kubectl config use-context mdenno@teehr-spark-cluster.us-east-2.eksctl.io
```

```
kubectl -n teehr-spark-default port-forward service/spark-iceberg 8888
kubectl -n teehr-spark-default port-forward pod/spark-master-0 8080
kubectl -n teehr-spark-default port-forward service/jupyter 8888
kubectl -n teehr-spark-default port-forward service/minio 9001
```

eksctl delete cluster --name teehr-spark-cluster --force --disable-nodegroup-eviction


eksctl scale nodegroup --cluster=teehr-spark-cluster --nodes=1 --nodes-max=4 --nodes-max=4 worker-ng

eksctl get nodegroup --cluster teehr-spark-cluster

## ---- Shouldn't need stuff below here -----
```
eksctl utils associate-iam-oidc-provider --region us-east-2 --cluster teehr-spark-cluster  --approve
```

```
eksctl create iamserviceaccount \
  --region us-east-2\
  --name ebs-csi-controller-sa \
  --namespace kube-system \
  --cluster teehr-spark-cluster  \
  --attach-policy-arn arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy \
  --approve \
  --role-only \
  --role-name AmazonEKS_EBS_CSI_DriverRole
```

```
eksctl create addon \
    --name aws-ebs-csi-driver 
    --cluster teehr-spark-cluster 
    --service-account-role-arn arn:aws:iam::$(aws sts get-caller-identity --query Account --output text):role/AmazonEKS_EBS_CSI_DriverRole 
    --force
```

```
kubectl apply -f autoscaler/cluster-autoscaler-autodiscover.yaml
```
```
kubectl delete -f autoscaler/cluster-autoscaler-autodiscover.yaml
```