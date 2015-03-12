Javadoc
=======

See project [javadoc](http://oryxproject.github.io/oryx/apidocs/index.html).

Bundled Serving Layer Apps
==========================

Oryx bundles several end-to-end applications, including a Serving Layer with REST endpoints.
The bundled app endpoints are:

Collaborative filtering / Recommendation
----------------------------------------

* [`/recommend`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/Recommend.html)
* [`/recommendToMany`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/RecommendToMany.html)
* [`/recommendToAnonymous`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/RecommendToAnonymous.html)
* [`/similarity`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/Similarity.html)
* [`/similarityToItem`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/SimilarityToItem.html)
* [`/knownItems`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/KnownItems.html)
* [`/estimate`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/Estimate.html)
* [`/estimateForAnonymous`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/EstimateForAnonymous.html)
* [`/because`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/Because.html)
* [`/mostSurprising`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/MostSurprising.html)
* [`/popularRepresentativeItems`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/PopularRepresentativeItems.html)
* [`/mostPopularItems`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/MostPopularItems.html)
* [`/mostActiveUsers`] (http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/MostActiveUsers.html)
* [`/item/allIDs`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/AllItemIDs.html)
* [`/ready`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/Ready.html)
* [`/pref`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/Preference.html)
* [`/ingest`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/Ingest.html)

Classification / Regression
---------------------------

* [`/predict`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/rdf/Predict.html)
* [`/classificationDistribution`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/rdf/ClassificationDistribution.html)
* [`/train`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/rdf/Train.html)

Clustering
----------

* [`/assign`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/kmeans/Assign.html)
* [`/distanceToNearest`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/kmeans/DistanceToNearest.html)
* [`/add`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/kmeans/Add.html)
