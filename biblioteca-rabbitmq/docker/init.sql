-- Cada responsabilidade possui suas próprias tabelas (efeitos verificáveis por consulta).
-- A PK em event_id garante idempotência: o mesmo evento nunca produz dois efeitos.

CREATE TABLE IF NOT EXISTS circulacao_saidas (
  event_id UUID PRIMARY KEY,
  emprestimo_id TEXT,
  exemplar_id TEXT,
  leitor_id TEXT,
  data_prevista_devolucao DATE,
  registrado_em TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS comunicacao_avisos (
  event_id UUID PRIMARY KEY,
  tipo_aviso TEXT NOT NULL,
  leitor_id TEXT,
  leitor_nome TEXT,
  mensagem TEXT NOT NULL,
  registrado_em TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS multas_registro (
  event_id UUID PRIMARY KEY,
  multa_id TEXT,
  situacao TEXT NOT NULL,
  leitor_id TEXT,
  valor NUMERIC(10,2),
  ocorrido_em TIMESTAMPTZ,
  registrado_em TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS auditoria_eventos (
  event_id UUID PRIMARY KEY,
  tipo TEXT NOT NULL,
  ocorrido_em TIMESTAMPTZ NOT NULL,
  routing_key TEXT,
  payload JSONB NOT NULL,
  registrado_em TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_auditoria_tipo ON auditoria_eventos (tipo);
CREATE INDEX IF NOT EXISTS idx_auditoria_ocorrido ON auditoria_eventos (ocorrido_em);

CREATE TABLE IF NOT EXISTS estatisticas_processados (
  event_id UUID PRIMARY KEY,
  instancia TEXT NOT NULL,
  processado_em TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS estatisticas_categoria (
  categoria TEXT PRIMARY KEY,
  total BIGINT NOT NULL
);
