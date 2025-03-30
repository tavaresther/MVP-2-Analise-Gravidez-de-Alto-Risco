# MVP-2-Analise-Gravidez-de-Alto-Risco

##Objetivo da Analise

Identificar atraves do dataset maternal_health quais são os principais fatores que estão relacionados a uma grávidez de alto risco, utilizando para embasamento as seguintes perguntas de negócio:

1. Quais características demográficas (idade, peso, altura) estão mais associadas a gestações de alto risco?
2. Como o histórico de anemia ou icterícia impacta a probabilidade de uma gravidez ser considerada de alto risco?
3. Existe uma correlação entre a pressão arterial elevada (hipertensão) e a gravidez de alto risco?
4. A presença de resultados positivos no teste de VDRL está associada a uma maior probabilidade de uma gravidez de alto risco?
5. Quais posições fetais mais frequentemente resultam em gravidez de alto risco?
6. Como a quantidade de movimentos fetais e os batimentos cardíacos fetais influenciam o risco da gravidez?
7. Como a idade influencia a classificação de risco durante a gravidez? Existem faixas etárias específicas que têm maior risco de complicações?
8. Gravidez de alto risco é mais comum em mulheres jovens ou mais velhas? Ou ambas as faixas etárias apresentam maior risco por diferentes razões?
9. Qual a relação entre o peso da gestante e a classificação de risco? Mulheres com excesso de peso têm mais chances de ter uma gravidez de alto risco?
10. O índice de massa corporal (IMC), que pode ser derivado do peso e altura, tem correlação com a classificação de risco?
11. Quais são as taxas de gravidez de alto risco entre as mulheres com resultados positivos nos testes de albumina ou açúcar na urina?
12. Qual é a incidência de gravidez de alto risco em mulheres com resultados anormais nos testes de urina (albumina e açúcar)?
13. Como os testes de sangue (VDLR, HRSAG) afetam a classificação de risco durante a gravidez?
14. A posição fetal tem algum impacto na classificação de risco para a mãe?
15. Qual é o impacto dos batimentos cardíacos fetais sobre o risco da gravidez?
16. Movimentos fetais reduzidos estão correlacionados com um aumento no risco para a mãe e/ou bebê?
17. Qual é a prevalência de anemia entre mulheres com gravidez de alto risco?
18. A presença de icterícia durante a gravidez está associada ao risco aumentado de complicações durante o parto?
19. Mulheres com histórico de hipertensão ou diabetes têm maior probabilidade de ter uma gravidez de alto risco?
20. Como a pressão arterial alta (hipertensão) influencia as taxas de complicações nas gestações de alto risco?
21. Como a taxa de gravidezes de alto risco muda ao longo dos trimestres, especialmente considerando a evolução das condições de saúde?
22. Quais são as tendências em termos de gestantes com alto risco baseadas nas características demográficas?
23. Mulheres com peso elevado ou histórico de anemia apresentam maiores chances de gravidez de alto risco?
24. Como fatores como idade, peso e histórico médico (anemia, icterícia) podem ser modificados para reduzir o risco durante a gravidez?
25. Quais intervenções podem ser mais eficazes para mulheres com características específicas (por exemplo, altas taxas de VDRL ou hipertensão) para prevenir complicações na gravidez?
26. Quais fatores podem ser ajustados para reduzir o risco de uma gravidez de alto risco, como a alimentação, monitoramento da pressão arterial ou controle da glicose?

## Informações sobre o Dataset

A base utilizada foi adquirida no site kaggle e renomeada para maternal_health e tranformada em csv.

### Fonte do Dataset: 
https://www.kaggle.com/datasets/ankurray00/maternal-health-and-high-risk-pregnancy-dataset

### Descrição do Dataset: 
(Traduzida do Kaggle)
Este conjunto de dados fornece informações abrangentes sobre os indicadores de saúde materna durante a gravidez, incluindo detalhes como idade, gravídica (número de gestações), peso, altura, pressão arterial, idade gestacional e estado de saúde fetal. Ele também inclui resultados de exames médicos importantes, como anemia, níveis de açúcar no sangue e frequência cardíaca fetal, oferecendo uma visão completa sobre a saúde na gravidez. O conjunto de dados classifica as gestações como de alto risco ou não de alto risco com base em fatores como resultados de exames, pressão arterial e bem-estar fetal. Coletado manualmente a partir de registros médicos, cada entrada foi cuidadosamente revisada e anonimizada para garantir a privacidade e a precisão. Este conjunto de dados serve como um recurso valioso para pesquisas em saúde materna, previsão de risco e resultados da gravidez, apoiando esforços para melhorar o cuidado pré-natal e estratégias de intervenção precoce.

## Indentificação das colunas:
Os nomes das colunas estavam um pouco dificeis de identificar, então ajustei para que ficassem mais fácil de entender e fui atras do significado de cada uma:
1. Name -> name: Identificação da paciente
2. Age -> idade: Idade da paciente
3. Gravida -> pregnancy: Essa coluna se trata de que gravidez a paciente está, primeira, segunda,...
4. Ti Ti Tika -> Pelo nome não é possivel identificar e não consegui identificar os dados, inicialmente achei que se tratav do trimestre da gestação mas não bate com a procima coluna, será desconsiderada
5. গর্ভকাল -> weeks: (Traduzido do Bengali ) Semanas de gestação
6. ওজন -> weight_kg: (Traduzido do Bengali) Peso da paciente
7. উচ্চতা -> height_ft: (Traduzido do Bengali) Altura da paciente
8. রক্ত চাপ -> blood_pressure: (Traduzido do Bengali) Pressão Alrterial
9. রক্তস্বল্পতা -> anemia: (Traduzido do Bengali) Se não possui anemia(null) ou o nivel da anemia da paciente
10. জন্ডিস -> jaundice: (Traduzido do Bengali) Conhecida como icterícia, é uma condição médica caracterizada pela coloração amarelada da pele, mucosas e olhos devido ao acúmulo de bilirrubina no organismo. A coluna informa se não possui icterícia(null) ou o nivel da anemia da paciente
11. গর্ভস্হ শিশু অবস্থান -> fetal_position: (Traduzido do Bengali) Informa se a posição do feto no utero está normal
12. গর্ভস্হ শিশু নাড়াচাড়া -> fetal_movements: (Traduzido do Bengali) Informa se os movimentos do feto estão normal
13. গর্ভস্হ শিশু হৃৎস্পন্দন -> fetal_heartbeat: (Traduzido do Bengali) Informa quantidade de batimentos cardiacos por minuto do feto
14. প্রসাব পরিক্ষা এলবুমিন -> urine_test_albumin: (Traduzido do Bengali) Informa se a quantidade de albumina na urina está normal, se não estiver pode ser um sinal de que os rins não estão funcionando corretamente
15. প্রসাব পরিক্ষা সুগার -> urine_test_sugar: (Traduzido do Bengali) Informa se a quantidade de açucar na urina está normal, se não estiver pode ser um sinal de que os rins não estão conseguindo filtrar o que pode significar diabetes
16. VDRL -> vdrl: Detecta a presença de anticorpos contra a Siflis
17. HRsAG -> hrsag: Detecta a presença de anticorpos contra a Hepatit B
18. ঝুকিপূর্ণ গর্ভ -> high_risk_pregnancy: (Traduzido do Bengali) Identifica qual o risco da gravidez, o que determina a quantidade de cuidados que serão necessários

