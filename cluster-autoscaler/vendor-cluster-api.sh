#!/bin/bash

# Put the cluster-api under internal (so it can not be imported outside of the clusterapi package)
mkdir -p cloudprovider/clusterapi/internal
cd cloudprovider/clusterapi/internal

# Clean the workspace
rm -rf vendor
mkdir vendor
cd vendor
mkdir sigs.k8s.io
(cd sigs.k8s.io; git clone --depth=1 https://github.com/kubernetes-sigs/cluster-api; cd cluster-api; rm -rf .git)
mv sigs.k8s.io/cluster-api/vendor/* .
rmdir sigs.k8s.io/cluster-api/vendor

# clean up duplicates which result in panics at runtime
rm -rf github.com/golang/glog
rm -rf golang.org/x/net
