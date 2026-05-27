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
  {{- if .Values.modularSdk.mongoUri }}
    value: "{{ .Values.modularSdk.mongoUri }}"
  {{- else }}
    value: "mongodb://$(MODULAR_SDK_MONGO_USER):$(MODULAR_SDK_MONGO_PASSWORD)@{{ .Values.mongoService }}:{{ .Values.mongoPort }}/"
  {{- end }}
  - name: MODULAR_SDK_MONGO_DB_NAME
    value: "{{ .Values.modularSdk.databaseName }}"
  - name: MODULAR_SDK_SERVICE_MODE
    value: {{ default "docker" .Values.modularSdk.serviceMode }}
  {{- if .Values.modularSdk.dbBackend }}
  - name: MODULAR_SDK_DB_BACKEND
    value: "{{ .Values.modularSdk.dbBackend }}"
  {{- end }}
  {{- if .Values.modularSdk.secretsBackend }}
  - name: MODULAR_SDK_SECRETS_BACKEND
    value: "{{ .Values.modularSdk.secretsBackend }}"
  {{- end }}
  {{- if ne (default "docker" .Values.modularSdk.serviceMode) "docker" }}
  - name: MODULAR_SDK_ASSUME_ROLE_ARN
    value: {{ .Values.modularSdk.assumeRoleArn }}
  - name: MODULAR_SDK_ASSUME_ROLE_REGION
    value: {{ .Values.modularSdk.awsRegion }}
  {{- end }}
  - name: MODULAR_SDK_LOG_LEVEL
    value: {{ .Values.modularSdk.logLevel }}
  - name: MODULAR_SDK_VAULT_URL
    value: "http://{{ .Values.modularSdk.vaultService }}:{{ .Values.modularSdk.vaultPort }}"
  - name: MODULAR_SDK_VAULT_TOKEN
    valueFrom:
      secretKeyRef:
        name: {{ default "vault-secret" .Values.modularSdk.vaultSecretName }}
        key: token
  - name: SRE_SERVICE_MODE
    value: docker
  - name: SRE_MONGO_URI
  {{- if .Values.mongoUri }}
    value: "{{ .Values.mongoUri }}"
  {{- else }}
    value: "mongodb://$(MODULAR_SDK_MONGO_USER):$(MODULAR_SDK_MONGO_PASSWORD)@{{ .Values.mongoService }}:{{ .Values.mongoPort }}/"
  {{- end }}
  - name: SRE_MONGO_DB_NAME
    value: "{{ .Values.databaseName }}"
  - name: SRE_VAULT_ENDPOINT
    value: "http://{{ .Values.vaultService }}:{{ .Values.vaultPort }}"
  - name: SRE_VAULT_TOKEN
    valueFrom:
      secretKeyRef:
        name: vault-secret
        key: token
  - name: SRE_MINIO_ENDPOINT
    value: "http://{{ .Values.minioService }}:{{ .Values.minioPort }}"
  - name: SRE_MINIO_ACCESS_KEY_ID
    valueFrom:
      secretKeyRef:
        name: minio-secret
        key: username
  - name: SRE_MINIO_SECRET_ACCESS_KEY
    valueFrom:
      secretKeyRef:
        name: minio-secret
        key: password
  - name: SRE_INNER_CACHE_TTL_SECONDS
    value: {{ .Values.innerCacheTTLSeconds | quote }}
  - name: SRE_SYSTEM_USER_PASSWORD
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
  - name: SRE_LOG_LEVEL
    value: {{ .Values.logLevel }}
  - name: SRE_GUNICORN_WORKERS
    value: {{ .Values.workers | quote}}
  {{- if .Values.metricsExpirationDays }}
  - name: SRE_METRICS_EXPIRATION_DAYS
    value: {{ .Values.metricsExpirationDays | quote }}
  {{- end }}
  {{- if .Values.dojoPayloadSizeLimitBytes }}
  - name: SRE_DOJO_PAYLOAD_SIZE_LIMIT_BYTES
    value: {{ .Values.dojoPayloadSizeLimitBytes | quote }}
  {{- end }}
  - name: SRE_BATCH_JOB_LOG_LEVEL
    value: {{ .Values.executorLogLevel }}
  {{- if .Values.allowSimultaneousJobsForOneTenant }}
  - name: SRE_ALLOW_SIMULTANEOUS_JOBS_FOR_ONE_TENANT
    value: 'true'
  {{- end  }}
  {{- if .Values.recommendationsBucket }}
  - name: SRE_RECOMMENDATIONS_BUCKET_NAME
    value: {{ .Values.recommendationsBucket }}
  {{- end }}
  {{- if .Values.minioPresignedUrl }}
  - name: SRE_MINIO_PRESIGNED_URL_HOST
    value: {{ .Values.minioPresignedUrl | quote }}
  {{- end }}
  {{- if .Values.minioPresignedUrlPublicIpv4 }}
  - name: SRE_MINIO_PRESIGNED_URL_PUBLIC_IPV4
    value: {{ .Values.minioPresignedUrlPublicIpv4 | quote }}
  {{- end }}
  {{- if .Values.minioPresignedUrlPrivateIpv4 }}
  - name: SRE_MINIO_PRESIGNED_URL_PRIVATE_IPV4
    value: {{ .Values.minioPresignedUrlPrivateIpv4 | quote }}
  {{- end }}
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
  - name: SRE_CELERY_LOG_LEVEL
    value: {{ .Values.celery.logLevel }}
  - name: SRE_CELERY_MAKE_FINDINGS_SNAPSHOTS_SCHEDULE
    value: '{{ .Values.celery.schedule.makeFindingsSnapshots }}'
  - name: SRE_CELERY_SYNC_LICENSE_SCHEDULE
    value: '{{ .Values.celery.schedule.syncLicense }}'
  - name: SRE_CELERY_COLLECT_METRICS_SCHEDULE
    value: '{{ .Values.celery.schedule.collectMetrics }}'
  - name: SRE_CELERY_REMOVE_EXPIRED_METRICS_SCHEDULE
    value: '{{ .Values.celery.schedule.removeExpiredMetrics }}'
  - name: SRE_CELERY_SCAN_RESOURCES_SCHEDULE
    value: '{{ .Values.celery.schedule.scanResources }}'
  - name: SRE_CELERY_ASSEMBLE_EVENTS_SCHEDULE
    value: '{{ .Values.celery.schedule.assembleEvents }}'
  - name: SRE_CELERY_CLEAR_EVENTS_SCHEDULE
    value: '{{ .Values.celery.schedule.clearEvents }}'
  - name: SRE_CC_LOG_LEVEL
    value: {{ .Values.executorCCLogLevel }}
  {{- if .Values.executorLogsFilename }}
  - name: SRE_EXECUTOR_LOGS_FILENAME
    value: {{ .Values.executorLogsFilename }}
  - name: SRE_ENABLE_CUSTOM_CC_PLUGINS # TODO Unset if value is empty
    value: {{ default "" .Values.celery.enableCustomCcPlugins | quote }}
{{- end }}
{{- end -}}
