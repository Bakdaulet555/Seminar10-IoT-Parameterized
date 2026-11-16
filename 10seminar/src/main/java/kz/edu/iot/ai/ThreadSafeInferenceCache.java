package kz.edu.iot.ai;

import java.util.Arrays;

public class ThreadSafeInferenceCache {

    private double[] lastSensorVector;
    private double[] lastPredictionProbs;

    private long totalRequests = 0;
    private long cacheHits = 0;

    public synchronized double getCacheHitRatio() {
        return totalRequests == 0 ? 0.0 :
                (double) cacheHits / totalRequests;
    }

    public synchronized long getTotalRequests() {
        return totalRequests;
    }

    public double[] predictAnomaly(double[] sensorVector) {
        if (sensorVector == null || sensorVector.length == 0) {
            throw new IllegalArgumentException("Empty sensor vector");
        }

        double[] input = sensorVector.clone();

        // Первая короткая критическая секция
        synchronized (this) {
            totalRequests++;

            if (lastSensorVector != null &&
                    Arrays.equals(input, lastSensorVector)) {
                cacheHits++;
                return lastPredictionProbs.clone();
            }
        }

        // Вычисление вне блокировки
        double[] result = runNeuralNetworkInference(input);

        // Вторая короткая критическая секция
        synchronized (this) {
            lastSensorVector = input.clone();
            lastPredictionProbs = result.clone();
        }

        return result;
    }

    private double[] runNeuralNetworkInference(double[] input) {
        double[] probabilities = new double[3];
        double sum = Arrays.stream(input).sum();
        double sumExp = 0.0;

        for (int i = 0; i < 3; i++) {
            probabilities[i] = Math.exp(sum * (i + 1) * 0.01);
            sumExp += probabilities[i];
        }

        for (int i = 0; i < 3; i++) {
            probabilities[i] /= sumExp;
        }

        return probabilities;
    }
}
