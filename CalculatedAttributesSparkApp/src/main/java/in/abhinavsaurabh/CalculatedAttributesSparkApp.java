package in.abhinavsaurabh;

import in.abhinavsaurabh.processors.CalculatedAttributesProcessor;

public class CalculatedAttributesSparkApp {

    public static void main(String[] args) {

        CalculatedAttributesProcessor formulaProcessor = new CalculatedAttributesProcessor();
        formulaProcessor.process();
    }
}