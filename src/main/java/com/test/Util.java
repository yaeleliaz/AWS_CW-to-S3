package com.test;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

import com.google.gson.Gson;

public class Util {
    public static void logEnvironment(Object event, Context context, Gson gson) {
        LambdaLogger logger = context.getLogger();
        logger.log("ENVIRONMENT VARIABLES: " + gson.toJson(System.getenv()) + "\n");
        logger.log("CONTEXT: " + gson.toJson(context) + "\n");
        logger.log("EVENT: " + gson.toJson(event) + "\n");
        logger.log("EVENT TYPE: " + event.getClass().toString() + "\n");
    }
}