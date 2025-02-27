{{- define "rule-engine.envs" -}}
env:
  - name: MODULAR_SDK_APPLICATION_NAME
    value: 'syndicate-rule-engine'
  - name: MODULAR_SDK_MONGO_USER
    valueFrom:
      secretKeyRef:
        name: mongo-secret
        key: username
  - name: MODULAR_SDK_MONGO_PASSWORD
    valueFrom:
      secretKeyRef:
        name: mongo-secret
        key: password
  - name: MODULAR_SDK_MONGO_URI
    value: "mongodb://$(MODULAR_SDK_MONGO_USER):$(MODULAR_SDK_MONGO_PASSWORD)@{{ .Values.mongoService }}:{{ .Values.mongoPort }}/"
  - name: MODULAR_SDK_MONGO_DB_NAME
    valueFrom:
      configMapKeyRef:
        name: {{ include "rule-engine.fullname" . }}
        key: modular-db-name
  - name: MODULAR_SDK_SERVICE_MODE
    value: {{ default "docker" .Values.modularSdk.serviceMode }}
  {{- if ne (default "docker" .Values.modularSdk.serviceMode) "docker" }}
  - name: MODULAR_SDK_ASSUME_ROLE_ARN
    value: {{ .Values.modularSdk.assumeRoleArn }}
  - name: MODULAR_SDK_ASSUME_ROLE_REGION
    value: {{ .Values.modularSdk.awsRegion }}
  {{- end }}
  - name: MODULAR_SDK_LOG_LEVEL
    value: {{ .Values.modularSDKLogLevel }}
  - name: MODULAR_SDK_VAULT_URL
    value: "http://{{ .Values.vaultService }}:{{ .Values.vaultPort }}"
  - name: MODULAR_SDK_VAULT_TOKEN
    valueFrom:
      secretKeyRef:
        name: vault-secret
        key: token
  - name: CAAS_SERVICE_MODE
    value: docker
  - name: CAAS_MONGO_URI
    value: "mongodb://$(MODULAR_SDK_MONGO_USER):$(MODULAR_SDK_MONGO_PASSWORD)@{{ .Values.mongoService }}:{{ .Values.mongoPort }}/"
  - name: CAAS_MONGO_DATABASE
    valueFrom:
      configMapKeyRef:
        name: {{ include "rule-engine.fullname" . }}
        key: db-name
  - name: CAAS_VAULT_ENDPOINT
    value: "http://{{ .Values.vaultService }}:{{ .Values.vaultPort }}"
  - name: CAAS_VAULT_TOKEN
    valueFrom:
      secretKeyRef:
        name: vault-secret
        key: token
  - name: CAAS_MINIO_ENDPOINT
    value: "http://{{ .Values.minioService }}:{{ .Values.minioPort }}"
  - name: CAAS_MINIO_ACCESS_KEY_ID
    valueFrom:
      secretKeyRef:
        name: minio-secret
        key: username
  - name: CAAS_MINIO_SECRET_ACCESS_KEY
    valueFrom:
      secretKeyRef:
        name: minio-secret
        key: password
  - name: CAAS_INNER_CACHE_TTL_SECONDS
    valueFrom:
      configMapKeyRef:
        name: {{ include "rule-engine.fullname" . }}
        key: inner-cache-ttl-seconds
  - name: CAAS_SYSTEM_USER_PASSWORD
    valueFrom:
      secretKeyRef:
        name: rule-engine-secret
        key: system-password
  - name: HTTP_PROXY
    value: {{ .Values.httpProxy }}
  - name: HTTPS_PROXY
    value: {{ .Values.httpProxy }}
  - name: NO_PROXY
    value: {{ .Values.noProxy }}
  - name: CAAS_LOG_LEVEL
    value: {{ .Values.logLevel }}
  - name: CAAS_BATCH_JOB_LOG_LEVEL
    value: {{ .Values.executorLogLevel }}
  {{- if .Values.allowSimultaneousJobsForOneTenant }}
  - name: CAAS_ALLOW_SIMULTANEOUS_JOBS_FOR_ONE_TENANT
    value: 'true'
  {{- end  }}
  {{- if .Values.recommendationsBucket }}
  - name: CAAS_RECOMMENDATIONS_BUCKET_NAME
    value: {{ .Values.recommendationsBucket }}
  {{- end}}
  - name: REDIS_PASSWORD
    valueFrom:
      secretKeyRef:
        name: redis-secret
        key: password
        optional: true
  - name: REDIS_DOMAIN
    value: {{ .Values.redisService }}
  - name: REDIS_PORT
    value: "{{ .Values.redisPort }}"
{{- end -}}