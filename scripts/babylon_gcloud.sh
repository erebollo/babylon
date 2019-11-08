#!/bin/bash

# bucket creation
gsutil mb gs://babylon-storage

# topic creation
gcloud pubsub topics create babylon-topic

