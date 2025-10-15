import cors from 'cors';
import express from 'express';
import FormData from 'form-data';
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

// Habilitar CORS
app.use(cors());

// Função para buscar dados
async function fetchFormData(formUid) {
  const url = `https://kf.kobotoolbox.org/api/v2/assets/${formUid}/data.json`;
  try {
    const res = await fetch(url, { headers: { Authorization: `Token ${KOBO_TOKEN}` } });
    if (!res.ok) throw new Error(`Erro ao buscar ${formUid}: ${res.status} ${res.statusText}`);
    return (await res.json()).results || [];
  } catch (error) {
    console.error(`Falha ao buscar dados para UID ${formUid}:`, error.message);
    return [];
  }
}

// Função para upload de CSV
// Função para upload de CSV usando API v1/metadata.json
// ✅ FUNÇÃO CORRIGIDA - Usando endpoint v1/metadata
async function uploadCSVToForm(formUid, filePath) {
  const url = `https://kf.kobotoolbox.org/api/v2/assets/${formUid}/files/`;
  if (!fs.existsSync(filePath)) {
    console.error(`❌ Arquivo ${filePath} não encontrado`);
    return;
  }
  console.log(`📄 Arquivo ${filePath} encontrado, tamanho: ${fs.statSync(filePath).size} bytes`);

  const formData = new FormData();
  formData.append('data_file', fs.createReadStream(filePath), { 
    filename: 'escolas.csv', 
    contentType: 'text/csv' 
  });
  formData.append('file_type', 'form_media');
  formData.append('description', 'Lista de escolas atualizada');

  try {
    // Verificar arquivos existentes
    const existingFilesRes = await fetch(url, { 
      headers: { Authorization: `Token ${KOBO_TOKEN}` } 
    });
    if (!existingFilesRes.ok) {
      throw new Error(`Erro ao listar arquivos para ${formUid}: ${existingFilesRes.status} ${await existingFilesRes.text()}`);
    }
    const filesData = await existingFilesRes.json();
    const existingFile = filesData.results?.find(file => file.metadata?.filename === 'escolas.csv');

    if (existingFile) {
      const deleteRes = await fetch(`${url}${existingFile.uid}/`, { 
        method: 'DELETE', 
        headers: { Authorization: `Token ${KOBO_TOKEN}` } 
      });
      if (!deleteRes.ok) {
        console.warn(`Falha ao deletar arquivo existente para ${formUid}: ${deleteRes.status} ${await deleteRes.text()}`);
      } else {
        console.log(`✅ Arquivo existente deletado para ${formUid}`);
      }
    }

    // Upload do CSV
    console.log(`📤 Enviando CSV para ${formUid}...`);
    const res = await fetch(url, {
      method: 'POST',
      headers: { Authorization: `Token ${KOBO_TOKEN}` },
      body: formData,
    });
    if (!res.ok) {
      const errorText = await res.text();
      console.error(`Resposta completa do servidor: ${errorText}`);
      throw new Error(`Erro ao fazer upload para ${formUid}: ${res.status} ${errorText}`);
    }
    console.log(`📤 CSV enviado com sucesso para ${formUid}`);

    // Reimplantar formulário
    const deployRes = await fetch(`https://kf.kobotoolbox.org/api/v2/assets/${formUid}/deployment/`, {
      method: 'POST',
      headers: { 
        Authorization: `Token ${KOBO_TOKEN}`, 
        'Content-Type': 'application/json' 
      },
      body: JSON.stringify({ active: true }),
    });
    if (deployRes.ok) {
      console.log(`✅ Formulário ${formUid} reimplantado`);
    } else {
      console.warn(`Falha na reimplantação para ${formUid}: ${deployRes.status} ${await deployRes.text()}`);
    }
  } catch (err) {
    console.error(`❌ Falha no upload do CSV para ${formUid}:`, err.message);
  }
}

// Upload para todos os formulários
async function uploadCSVToAllForms(filePath) {
  for (const [name, uid] of Object.entries(FORMS)) {
    console.log(`➡️ Subindo CSV para ${name} (${uid})...`);
    await uploadCSVToForm(uid, filePath);
  }
}

// Função para consolidar escolas e salvar CSV

async function generateCSV() {
  let escolasMap = new Map();

  for (const [table, uid] of Object.entries(FORMS)) {
    console.log(`Buscando dados para ${table} (${uid})...`);
    const submissions = await fetchFormData(uid);

    submissions.forEach((row) => {
      const codigo = row["identificacao_da_escola/DGE_SQE_B0_P1_codigo_escola"];
      const nome = row["identificacao_da_escola/DGE_SQE_B1_P1_nome_escola"] || "Sem nome";
      const provincia = row["identificacao_da_escola/DGE_SQE_B1_P4_provincia"] || "";
      const municipio = row["identificacao_da_escola/DGE_SQE_B1_P5_municipio"] || "";
      const comuna = row["identificacao_da_escola/DGE_SQE_B1_P7_localidade"] || "";

      // --- Campos adicionais ---
      const inicioAno = row["identificacao_da_escola/DGE_SQE_B0_P2_inicio_ano_lectivo"] || "";
      const fimAno = row["identificacao_da_escola/DGE_SQE_B0_P3_fim_ano_lectivo"] || "";
      const situacaoFunc = row["identificacao_da_escola/DGE_SQE_B1_P0_situacao_funcionamento"] || "";
      const endereco = row["identificacao_da_escola/DGE_SQE_B1_P2_endereco_escola"] || "";
      const referencia = row["identificacao_da_escola/DGE_SQE_B1_P3_ponto_referencia"] || "";
      const comunaDistrito = row["identificacao_da_escola/DGE_SQE_B1_P6_comuna_distrito"] || "";
      const natureza = row["identificacao_da_escola/DGE_SQE_B1_P8_natureza_da_escola"] || "";
      const zonaGeografica = row["identificacao_da_escola/DGE_SQE_B1_P9_area_residencia_zona_geografica"] || "";
      const temDecreto = row["identificacao_da_escola/DGE_SQE_B1_P9_escola_tem_decreto"] || "";
      const temLicenca = row["identificacao_da_escola/DGE_SQE_B1_P9_escola_tem_licenca"] || "";
      const decreto = row["identificacao_da_escola/DGE_SQE_B1_P10_decreto_criacao"] || "";
      const licenca = row["identificacao_da_escola/DGE_SQE_B1_P11_licenca"] || "";

      if (codigo && nome !== "Sem nome") {
        escolasMap.set(codigo, {
          codigo: codigo.toString().trim(),
          nome: nome.toString().trim(),
          provincia: provincia.toString().trim(),
          municipio: municipio.toString().trim(),
          comuna: comuna.toString().trim(),
          inicioAno: inicioAno.trim(),
          fimAno: fimAno.trim(),
          situacaoFunc: situacaoFunc.trim(),
          endereco: endereco.trim(),
          referencia: referencia.trim(),
          comunaDistrito: comunaDistrito.trim(),
          natureza: natureza.trim(),
          zonaGeografica: zonaGeografica.trim(),
          temDecreto: temDecreto.trim(),
          temLicenca: temLicenca.trim(),
          decreto: decreto.trim(),
          licenca: licenca.trim(),
        });
      } else {
        console.warn(`Submissão ignorada em ${table}: código=${codigo}, nome=${nome}`);
      }
    });
  }

  // --- Geração de CSV completo ---
  const headers = [
    "codigo",
    "nome",
    "provincia",
    "municipio",
    "comuna",
    "inicioAno",
    "fimAno",
    "situacaoFunc",
    "endereco",
    "referencia",
    "comunaDistrito",
    "natureza",
    "zonaGeografica",
    "temDecreto",
    "temLicenca",
    "decreto",
    "licenca",
  ];

  const escapeCSV = (str) => `"${(str || "").replace(/"/g, '""')}"`;

  const linhas = [headers.join(",")];

  escolasMap.forEach((e) => {
    const linha = headers.map((h) => escapeCSV(e[h] || ""));
    linhas.push(linha.join(","));
  });

  const publicDir = path.join(process.cwd(), "public");
  if (!fs.existsSync(publicDir)) fs.mkdirSync(publicDir, { recursive: true });

  const csvPath = path.join(publicDir, "escolas.csv");
  fs.writeFileSync(csvPath, linhas.join("\n"), "utf8");

  console.log(`✅ CSV completo salvo: ${csvPath}`);
  console.log(`📊 Total de escolas incluídas: ${escolasMap.size}`);
}



async function generateCSVXXX() {
  let escolasMap = new Map();

  for (const [table, uid] of Object.entries(FORMS)) {
    console.log(`Buscando dados para ${table} (${uid})...`);
    const submissions = await fetchFormData(uid);

    submissions.forEach((row) => {
      const codigo = row["identificacao_da_escola/DGE_SQE_B0_P1_codigo_escola"];
      const nome = row["identificacao_da_escola/DGE_SQE_B1_P1_nome_escola"] || "Sem nome";
      const provincia = row["identificacao_da_escola/DGE_SQE_B1_P4_provincia"] || "";
      const municipio = row["identificacao_da_escola/DGE_SQE_B1_P5_municipio"] || "";
      const comuna = row["identificacao_da_escola/DGE_SQE_B1_P7_localidade"] || "";





      if (codigo && nome !== "Sem nome") {
        escolasMap.set(codigo, {
          codigo: codigo.toString().trim(),
          nome: nome.toString().trim(),
          provincia: provincia.toString().trim(),
          municipio: municipio.toString().trim(),
          comuna: comuna.toString().trim(),
        });
      } else {
        console.warn(`Submissão ignorada em ${table}: código=${codigo}, nome=${nome}`);
      }
    });
  }

  // Gerar CSV no formato Kobo (list_name,name,label)
  const linhas = ["list_name,name,label"];
  escolasMap.forEach((e) => {
    const escapeCSV = (str) => `"${str.replace(/"/g, '""')}"`;

    // O label pode concatenar nome + localidade/município/província
    const label = `${e.nome} (${e.comuna || "?"} - ${e.municipio || "?"} - Prov. ${e.provincia || "?"})`;

    linhas.push(
      ["escolas", escapeCSV(e.codigo), escapeCSV(label)].join(",")
    );
  });

  const publicDir = path.join(process.cwd(), "public");
  if (!fs.existsSync(publicDir)) fs.mkdirSync(publicDir, { recursive: true });
  const csvPath = path.join(publicDir, "escolas.csv");

  fs.writeFileSync(csvPath, linhas.join("\n"), "utf8");
  console.log(`✅ CSV compatível com Kobo salvo: ${csvPath}, ${escolasMap.size} escolas incluídas`);

  // Upload para todos os formulários (se quiser)
  // await uploadCSVToAllForms(csvPath);
}




// Rota para servir o CSV
app.get("/csv/escolas.csv", (req, res) => {
  const csvPath = path.join(process.cwd(), "public", "escolas.csv");
  if (fs.existsSync(csvPath)) {
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', 'inline; filename="escolas.csv"');
    res.setHeader('Cache-Control', 'no-cache, must-revalidate');
    res.sendFile(csvPath);
  } else {
    res.status(404).send("CSV não encontrado");
  }
});


//executar primeira vez independente do cron
generateCSV().catch(console.error);

// Atualização automática a cada 5 minutos desde que generateCSV já tenha rodado uma vez
cron.schedule('*/5 * * * *', () => {
  console.log('⏰ Iniciando atualização automática do CSV...');
  generateCSV().catch(console.error);
});

// Iniciar servidor Express na porta definida       

app.listen(PORT, () => {
   // Primeira execução
  console.log(`🚀 Servidor rodando em http://localhost:${PORT}`);
  console.log(`📊 CSV disponível em: http://localhost:${PORT}/csv/escolas.csv`);
});