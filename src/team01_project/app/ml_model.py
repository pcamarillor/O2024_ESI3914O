from pyspark.ml.clustering import KMeans

def train_kmeans_model(feature_vector, k=2, seed=1):
    kmeans = KMeans(k=k, seed=seed)
    model = kmeans.fit(feature_vector)
    return model

def detect_anomalies(feature_vector, model):
    predictions = model.transform(feature_vector)
    anomalies = predictions.filter(predictions.prediction == 1)  # Supongamos que el cluster 1 es anómalo
    return anomalies
