set dotenv-load

version := `awk -F'"' '/^version = "/ { print $2; exit }' server/build.gradle.kts`
image := env_var_or_default("OPCDAPTER_IMAGE", "opcdapter")
image_tag := "v" + version
commit := `git describe --always --dirty --match '__no_such_tag__' 2>/dev/null || echo unknown`
datetime := `date +%Y-%m-%dT%H:%M:%S%z`

default:
  @just --list

gen:
  buf generate

go-test:
  GOWORK=off go test ./...

build:
  cd server && ./gradlew classes

test:
  cd server && ./gradlew test

lint:
  cd server && ./gradlew spotlessCheck

fmt:
  cd server && ./gradlew spotlessApply

ci:
  cd server && ./gradlew spotlessApply spotlessCheck test

check:
  cd server && ./gradlew check

deps:
  cd server && ./gradlew --no-configuration-cache dependencyUpdates

run *args:
  cd server && ./gradlew run {{args}}

clean:
  cd server && ./gradlew clean

docker:
  cd server && ./gradlew buildLayers dockerfile
  docker build --platform linux/amd64 \
    --build-arg VERSION={{version}} --build-arg COMMIT={{commit}} --build-arg DATETIME={{datetime}} \
    -t {{image}}:{{image_tag}} server/build/docker/main
  docker save {{image}}:{{image_tag}} -o opcdapter-{{image_tag}}.tar

# Live tests require OPCDA_HOST and exactly one of OPCDA_CLS_ID or OPCDA_PROG_ID.
opcda-time:
  cd server && ./gradlew test --tests "*OpcDaServerTimeTest" -Dopcda.it.enabled=true

opcda-read:
  cd server && ./gradlew test --tests "*OpcDaReadTest" -Dopcda.it.enabled=true

opcda-write:
  cd server && ./gradlew test --tests "*OpcDaWriteTest" -Dopcda.it.enabled=true

opcda-browse:
  cd server && ./gradlew --no-daemon test --tests "*OpcDaBrowseTest" -Dopcda.it.enabled=true
