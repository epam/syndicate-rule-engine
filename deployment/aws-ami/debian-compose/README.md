
`artifacts` folder does not contain all the artifacts. It lacks docker images that must be built.


```bash
$ ls
custodian-as-a-service
modular-service
m3-modular-admin
m3-modular-cli
django-DefectDojo
```


### Rule Engine image
```bash
cd custodian-as-a-service/
make open-source-server-image DOCKER_EXECUTABLE=docker
```

```bash
docker save caas-custodian-k8s-dev:latest | gzip > deployment/aws-ami/debian-compose/artifacts/rule-engine.tar.gz
```

### Obfuscation manager cli


```bash
make obfuscation-manager-dist
```

```bash
cp obfuscation_manager/dist/rule_engine_obfuscation_manager-*.tar.gz deployment/aws-ami/debian-compose/artifacts/rule_engine_obfuscation_manager.tar.gz
```


### Modular service image
```bash
cd ../modular-service
make image DOCKER_EXECUTABLE=docker
```

```bash
docker save modular-service:latest | gzip > ../custodian-as-a-service/deployment/aws-ami/debian-compose/artifacts/modular-service.tar.gz
```

### Modular api image
```bash
cd ../m3-modular-admin
mkdir ./docker_modules
cp -r ../custodian-as-a-service/c7n ./docker_modules/
cp -r ../modular-service/modular-service-cli ./docker_modules/
echo '{"module_name":"c7ncli","cli_path":"/c7ncli/group","mount_point":"/re"}' > ./docker_modules/c7n/api_module.json
echo '{"module_name":"modular-service-cli","cli_path":"/modular_service_cli/group","mount_point":"/admin"}' > ./docker_modules/modular-service-cli/api_module.json
make image DOCKER_EXECUTABLE=docker
```

```bash
docker save m3-modular-admin:latest | gzip > ../custodian-as-a-service/deployment/aws-ami/debian-compose/artifacts/modular-api.tar.gz
```


### Modular cli


```bash
cd ../m3-modular-cli
make dist
```

```bash
cp dist/modular_cli-*.tar.gz ../custodian-as-a-service/deployment/aws-ami/debian-compose/artifacts/modular_cli.tar.gz
```

### Defect Dojo images

Amd images exist in Docker Hub but `arm` images should be built manually


```bash
cd ../django-DefectDojo  # https://github.com/DefectDojo/django-DefectDojo.git
sed -i 's/target: django/target: django-alpine/g' docker-compose.yml  # to fix Dojo's compose error
DEFECT_DOJO_OS=alpine DJANGO_VERSION=latest docker compose build uwsgi
DEFECT_DOJO_OS=debian NGINX_VERSION=latest docker compose build nginx
```

```bash
docker save defectdojo/defectdojo-django:latest | gzip > ../custodian-as-a-service/deployment/aws-ami/debian-compose/artifacts/defectdojo-django.tar.gz
docker save defectdojo/defectdojo-nginx:latest | gzip > ../custodian-as-a-service/deployment/aws-ami/debian-compose/artifacts/defectdojo-nginx.tar.gz
```


### Making artifacts archive


```bash
cd ../custodian-as-a-service
cd deployment/aws-ami/debian-compose/artifacts
zip rule-engine-ami-artifacts.linux-$(dpkg --print-architecture).zip compose.yaml \
    defectdojo-django.tar.gz \
    defectdojo-nginx.tar.gz \
    dojo-compose.yaml \
    generate_random_envs.py \
    modular-api.tar.gz \
    modular-service.tar.gz \
    modular_cli.tar.gz \
    rule-engine.tar.gz \
    rule_engine_obfuscation_manager.tar.gz \
    sre-init.sh
cd ../../../..
```