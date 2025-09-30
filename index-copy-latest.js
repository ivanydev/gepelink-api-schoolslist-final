import express from 'express';
import fs from 'fs';
import cron from 'node-cron';
import path from 'path';

const app = express();
const PORT = 4000;

// 🔑 Token de API Kobo
const KOBO_TOKEN = "89ddaeaffdca763d95c616d09e5198c5af7bc8bd";

// Mapear formulários → UIDs
const FORMS = {
  "kobo_educacao_pre_escola_geral_adultos": "a9CFNEqESwyhkPQfiuumb4",
  "kobo_formacao_pedagogica": "aGRsksyVHmEQZiNRe62ShQ",
  "kobo_ensino_especial": "aTdQJqe3QKKaP5Ud4Ca33w",
  "kobo_ensino_tecnico_profissional": "aVLeAywfL3LJkKDfZSYmVx",
};

// Função para buscar dados de 1 formulário
async function fetchFormData(formUid) {
  const url = `https://kf.kobotoolbox.org/api/v2/assets/${formUid}/data.json`;
  try {
    const res = await fetch(url, {
      headers: { Authorization: `Token ${KOBO_TOKEN}` },
    });
    if (!res.ok) {
      throw new Error(`Erro ao buscar ${formUid}: ${res.status} ${res.statusText}`);
    }
    const data = await res.json();
    //console.log(`Dados recebidos para ${formUid}:`, JSON.stringify(data.results, null, 2));
    return data.results || [];
  } catch (error) {
    console.error(`Falha ao buscar dados para UID ${formUid}:`, error.message);
    return [];
  }
}

// Função para consolidar escolas
async function generateCSV() {
  let escolasMap = new Map();

  for (const [table, uid] of Object.entries(FORMS)) {
    console.log(`Buscando dados para ${table} (${uid})...`);
    const submissions = await fetchFormData(uid);

    submissions.forEach((row, index) => {
      //console.log(`Submissão ${index + 1} para ${table}:`, JSON.stringify(row, null, 2));
      // Ajustar nomes de campos conforme o JSON retornado
      const codigo = row["identificacao_da_escola/DGE_SQE_B0_P1_codigo_escola"];
      const nome = row["identificacao_da_escola/DGE_SQE_B1_P1_nome_escola"] || "Sem nome";
      const provincia = row["identificacao_da_escola/DGE_SQE_B1_P4_provincia"] || "";
      const municipio = row["identificacao_da_escola/DGE_SQE_B1_P5_municipio"] || "";
      const comuna = row["identificacao_da_escola/DGE_SQE_B1_P7_localidade"] || "";

      if (codigo) {
        escolasMap.set(codigo, { codigo, nome, provincia, municipio, comuna });
      } else {
        console.warn(`Submissão ignorada em ${table}: código=${codigo}, nome=${nome}`);
      }
    });
  }

  // Gerar CSV final
  const linhas = ["DGE_SQE_B0_P1_codigo_escola,DGE_SQE_B1_P1_nome_escola,DGE_SQE_B1_P4_provincia,DGE_SQE_B1_P5_municipio,DGE_SQE_B1_P7_localidade"];
  escolasMap.forEach((e) => {
    linhas.push(
      `${e.codigo},${e.nome},${e.provincia},${e.municipio},${e.comuna}`
    );
  });

  // Garantir que a pasta 'public' existe
  const publicDir = path.join(process.cwd(), "public");
  if (!fs.existsSync(publicDir)) {
    fs.mkdirSync(publicDir);
  }

  // Salvar CSV
  const csvPath = path.join(process.cwd(), "public", "escolas.csv");
  fs.writeFileSync(csvPath, linhas.join("\n"), "utf8");
  console.log(`✅ CSV atualizado: ${csvPath}, ${escolasMap.size} escolas incluídas`);
}

// Rota para servir o CSV
app.get("/csv/escolas.csv", (req, res) => {
  const csvPath = path.join(process.cwd(), "public", "escolas.csv");
  if (fs.existsSync(csvPath)) {
    res.sendFile(csvPath);
  } else {
    res.status(404).send("CSV não encontrado");
  }
});

// Rodar atualização automática a cada 10 segundos (para testes)
cron.schedule("*/30 * * * * *", () => {
  console.log("⏳ Atualizando CSV...");
  generateCSV().catch(console.error);
});

// Primeira geração ao iniciar servidor
generateCSV().catch(console.error);

app.listen(PORT, () => {
  console.log(`🚀 Servidor rodando em http://localhost:${PORT}`);
});