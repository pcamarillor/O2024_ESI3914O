-- Crear el keyspace para el análisis de tráfico de red
CREATE KEYSPACE IF NOT EXISTS network_traffic_data
WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 };

-- Usar el keyspace creado
USE network_traffic_data;

-- Tabla para almacenar los logs de tráfico crudos
CREATE TABLE IF NOT EXISTS traffic_logs (
  log_id uuid PRIMARY KEY,          -- Identificador único para cada entrada
  source_ip text,                   -- IP de origen
  destination_ip text,              -- IP de destino
  source_port int,                  -- Puerto de origen
  destination_port int,             -- Puerto de destino
  protocol text,                    -- Protocolo (TCP, UDP, etc.)
  timestamp timestamp,              -- Marca de tiempo del paquete
  packet_size int,                  -- Tamaño del paquete en bytes
  action text                       -- Acción (permitido, bloqueado, inspeccionado, etc.)
);

-- Tabla para almacenar resúmenes de tráfico
CREATE TABLE IF NOT EXISTS traffic_summary (
  summary_id uuid PRIMARY KEY,      -- Identificador único del resumen
  timestamp_hour timestamp,         -- Hora del resumen
  source_ip text,                   -- IP de origen
  destination_ip text,              -- IP de destino
  protocol text,                    -- Protocolo (TCP, UDP, etc.)
  total_packets int,                -- Total de paquetes capturados
  total_bytes int,                  -- Total de bytes transferidos
  action text                       -- Acción dominante (permitido, bloqueado, etc.)
);

-- Tabla para almacenar alertas generadas
CREATE TABLE IF NOT EXISTS alerts (
  alert_id uuid PRIMARY KEY,        -- Identificador único de la alerta
  timestamp timestamp,              -- Hora de la alerta
  source_ip text,                   -- IP de origen sospechoso
  destination_ip text,              -- IP de destino
  protocol text,                    -- Protocolo involucrado
  description text,                 -- Descripción de la alerta
  severity text                     -- Severidad de la alerta (low, medium, high)
);

-- Tabla para almacenar IPs bloqueadas
CREATE TABLE IF NOT EXISTS blocked_ips (
  ip text PRIMARY KEY,              -- Dirección IP bloqueada
  reason text,                      -- Razón por la que fue bloqueada
  block_time timestamp,             -- Hora en la que se bloqueó
  expiration_time timestamp         -- Hora en la que se desbloqueará (si aplica)
);

-- Tabla para almacenar reglas de tráfico
CREATE TABLE IF NOT EXISTS traffic_rules (
  rule_id uuid PRIMARY KEY,         -- Identificador único de la regla
  rule_description text,            -- Descripción de la regla
  source_ip text,                   -- IP de origen objetivo (opcional)
  destination_ip text,              -- IP de destino objetivo (opcional)
  protocol text,                    -- Protocolo objetivo
  action text,                      -- Acción a realizar (permitir, bloquear, alertar)
  created_at timestamp,             -- Hora de creación de la regla
  updated_at timestamp              -- Última actualización de la regla
);
