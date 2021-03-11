# push

MQTT -> 1883, HTTP -> 8080

mvn dependency:copy-dependencies -DoutputDirectory=lib -DincludeScope=runtime

java -Dloader.path=./lib -jar push-0.0.1.jar
