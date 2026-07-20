from eclipse-temurin:26-jdk-jammy as builder
workdir /workspace/app

copy gradlew .
copy gradle gradle
copy build.gradle .
copy settings.gradle .
copy src src

run ./gradlew bootJar -x test

workdir /workspace/app/extracted
run java -Djarmode=tools -jar ../build/libs/*.jar extract --layers --launcher --destination .

from eclipse-temurin:26-jdk-jammy
volume /tmp
workdir /app

copy --from=builder /workspace/app/extracted/dependencies/ ./
copy --from=builder /workspace/app/extracted/spring-boot-loader/ ./
copy --from=builder /workspace/app/extracted/snapshot-dependencies/ ./
copy --from=builder /workspace/app/extracted/application/ ./

entrypoint ["java", "org.springframework.boot.loader.launch.JarLauncher"]