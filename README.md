# MVP-2-Analise-Gravidez-de-Alto-Risco

## Fonte do Dataset: 
https://www.kaggle.com/datasets/ankurray00/maternal-health-and-high-risk-pregnancy-dataset

## Descrição do Dataset: 
(Traduzida do Kaggle)
Este conjunto de dados fornece informações abrangentes sobre os indicadores de saúde materna durante a gravidez, incluindo detalhes como idade, gravídica (número de gestações), peso, altura, pressão arterial, idade gestacional e estado de saúde fetal. Ele também inclui resultados de exames médicos importantes, como anemia, níveis de açúcar no sangue e frequência cardíaca fetal, oferecendo uma visão completa sobre a saúde na gravidez. O conjunto de dados classifica as gestações como de alto risco ou não de alto risco com base em fatores como resultados de exames, pressão arterial e bem-estar fetal. Coletado manualmente a partir de registros médicos, cada entrada foi cuidadosamente revisada e anonimizada para garantir a privacidade e a precisão. Este conjunto de dados serve como um recurso valioso para pesquisas em saúde materna, previsão de risco e resultados da gravidez, apoiando esforços para melhorar o cuidado pré-natal e estratégias de intervenção precoce.

## Indentificação das colunas:
Os nomes das colunas estavam muito errados, então ajustei para que ficassem mais fácil de entender e fui atras do significado de cada uma:
Name -> name: Identificação da paciente
Age -> idade: Idade da paciente
Gravida -> pregnancy: Essa coluna se trata de que gravidez a paciente está, primeira, segunda,...
Ti Ti Tika -> Pelo nome não é possivel identificar e não consegui identificar os dados, inicialmente achei que se tratav do trimestre da gestação mas não bate com a procima coluna, será desconsiderada
গর্ভকাল -> weeks: (Traduzido do Bengali ) Semanas de gestação
ওজন -> weight_kg: (Traduzido do Bengali) Peso da paciente
উচ্চতা -> height_ft: (Traduzido do Bengali) Altura da paciente
রক্ত চাপ -> blood_pressure: (Traduzido do Bengali) Pressão Alrterial
রক্তস্বল্পতা -> anemia: (Traduzido do Bengali) Se não possui anemia(null) ou o nivel da anemia da paciente
জন্ডিস -> jaundice: (Traduzido do Bengali) Conhecida como icterícia, é uma condição médica caracterizada pela coloração amarelada da pele, mucosas e olhos devido ao acúmulo de bilirrubina no organismo. A coluna informa se não possui icterícia(null) ou o nivel da anemia da paciente
গর্ভস্হ শিশু অবস্থান -> fetal_position: (Traduzido do Bengali) Informa se a posição do feto no utero está normal
গর্ভস্হ শিশু নাড়াচাড়া -> fetal_movements: (Traduzido do Bengali) Informa se os movimentos do feto estão normal
গর্ভস্হ শিশু হৃৎস্পন্দন -> fetal_heartbeat: (Traduzido do Bengali) Informa quantidade de batimentos cardiacos por minuto do feto
প্রসাব পরিক্ষা এলবুমিন -> urine_test_albumin: (Traduzido do Bengali) Informa se a quantidade de albumina na urina está normal, se não estiver pode ser um sinal de que os rins não estão funcionando corretamente
প্রসাব পরিক্ষা সুগার -> urine_test_sugar: (Traduzido do Bengali) Informa se a quantidade de açucar na urina está normal, se não estiver pode ser um sinal de que os rins não estão conseguindo filtrar o que pode significar diabetes
VDRL -> vdrl: Detecta a presença de anticorpos contra a Siflis
HRsAG -> hrsag: Detecta a presença de anticorpos contra a Hepatit B
ঝুকিপূর্ণ গর্ভ -> high_risk_pregnancy: (Traduzido do Bengali) Identifica qual o risco da gravidez, o que determina a quantidade de cuidados que serão necessários


