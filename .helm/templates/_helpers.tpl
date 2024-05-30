{{/*
Expand the name of the chart.
*/}}
{{- define "app.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "app.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := .Chart.Name }}
{{- if contains .Release.Name $name }}
{{- $name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "app.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "app.selectorLabels" -}}
app.kubernetes.io/name: {{ include "app.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "app.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "app.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Generage image reference based on image repository and tag
*/}}
{{- define "app.image" -}}
{{- printf "%s:%s" .Values.image.repository  (default (printf "%s" .Chart.AppVersion) .Values.image.tag) }}
{{- end }}

{{/*
Generage common labels
*/}}
{{- define "app.labels" -}}
helm.sh/chart: {{ include "app.chart" . }}
{{ include "app.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.additionalLabels }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Generate the job editor cluster role name
*/}}
{{- define "app.clusteRole.jobEditor" -}}
{{- if .Values.rbac.clusterRole.jobEditor.nameOverride }}
{{- .Values.rbac.clusterRole.jobEditor.nameOverride }}
{{- else }}
{{- printf "%s-job-editor" (include "app.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Generate the job viewer cluster role name
*/}}
{{- define "app.clusteRole.jobViewer" -}}
{{- if .Values.rbac.clusterRole.jobViewer.nameOverride }}
{{- .Values.rbac.clusterRole.jobViewer.nameOverride }}
{{- else }}
{{- printf "%s-job-viewer" (include "app.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Generate the job template editor cluster role name
*/}}
{{- define "app.clusteRole.jobTemplateEditor" -}}
{{- if .Values.rbac.clusterRole.jobTemplateEditor.nameOverride }}
{{- .Values.rbac.clusterRole.jobTemplateEditor.nameOverride }}
{{- else }}
{{- printf "%s-job-template-editor" (include "app.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Generate the job template viewer cluster role name
*/}}
{{- define "app.clusteRole.jobTemplateViewer" -}}
{{- if .Values.rbac.clusterRole.jobTemplateViewer.nameOverride }}
{{- .Values.rbac.clusterRole.jobTemplateViewer.nameOverride }}
{{- else }}
{{- printf "%s-job-template-viewer" (include "app.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Generate the job template editor cluster role name
*/}}
{{- define "app.clusteRole.streamClassEditor" -}}
{{- if .Values.rbac.clusterRole.streamClassEditor.nameOverride }}
{{- .Values.rbac.clusterRole.streamClassEditor.nameOverride }}
{{- else }}
{{- printf "%s-stream-class-editor" (include "app.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Generate the job template viewer cluster role name
*/}}
{{- define "app.clusteRole.streamClassViewer" -}}
{{- if .Values.rbac.clusterRole.streamClassViewer.nameOverride }}
{{- .Values.rbac.clusterRole.streamClassViewer.nameOverride }}
{{- else }}
{{- printf "%s-stream-class-viewer" (include "app.fullname" .) }}
{{- end }}
{{- end }}

