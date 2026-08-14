plugins {
    java
    application
}

description = "Mock FIX 4.2 Kafka producer for the local demo"

application {
    mainClass.set("com.deephaven.fix42.demo.FixDemoProducer")
}

dependencies {
    implementation(project(":fix-codec"))
    implementation(project(":oms-engine"))
    implementation(libs.kafka.clients)
}
