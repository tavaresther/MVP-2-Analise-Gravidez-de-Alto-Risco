# MVP-2-Analise-Gravidez-de-Alto-Risco

## Objetivo da Analise

Identificar atraves do dataset maternal_health quais são os principais fatores que estão relacionados a uma grávidez de alto risco, utilizando para embasamento as seguintes perguntas de negócio:

1. A idade está relacionada a gestações de alto risco?
2. A quantidade de vezes que a paciente ficou grávida está relacionada a gestações de alto risco?
3. O IMC está relacionada a gestações de alto risco?
4. Existe uma correlação entre a pressão arterial elevada (hipertensão) e a gravidez de alto risco?
5. Como o histórico de anemia impacta a probabilidade de uma gravidez ser considerada de alto risco?
6. Como o histórico de jaundice impacta a probabilidade de uma gravidez ser considerada de alto risco?
7. A posição fetal anormal é mais frequentemente em gravidez de alto risco?
8. Como o histórico de urine_test_albumin impacta a probabilidade de uma gravidez ser considerada de alto risco?
9. Como o histórico de urine_test_sugar impacta a probabilidade de uma gravidez ser considerada de alto risco?
10. Como o histórico de vdrl impacta a probabilidade de uma gravidez ser considerada de alto risco?
11. Como o histórico de hrsag impacta a probabilidade de uma gravidez ser considerada de alto risco?

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
10. জন্ডিস -> jaundice: (Traduzido do Bengali) Conhecida como icterícia, é uma condição médica caracterizada pela coloração amarelada da pele, mucosas e olhos devido ao acúmulo de bilirrubina no organismo. A coluna informa se não possui icterícia(null) ou o nivel da icterícia da paciente
11. গর্ভস্হ শিশু অবস্থান -> fetal_position: (Traduzido do Bengali) Informa se a posição do feto no utero está normal
12. গর্ভস্হ শিশু নাড়াচাড়া -> fetal_movements: (Traduzido do Bengali) Informa se os movimentos do feto estão normal
13. গর্ভস্হ শিশু হৃৎস্পন্দন -> fetal_heartbeat: (Traduzido do Bengali) Informa quantidade de batimentos cardiacos por minuto do feto
14. প্রসাব পরিক্ষা এলবুমিন -> urine_test_albumin: (Traduzido do Bengali) Informa se a quantidade de albumina na urina está normal, se não estiver pode ser um sinal de que os rins não estão funcionando corretamente
15. প্রসাব পরিক্ষা সুগার -> urine_test_sugar: (Traduzido do Bengali) Informa se a quantidade de açucar na urina está normal, se não estiver pode ser um sinal de que os rins não estão conseguindo filtrar o que pode significar diabetes
16. VDRL -> vdrl: Detecta a presença de anticorpos contra a Siflis
17. HRsAG -> hrsag: Detecta a presença de anticorpos contra a Hepatit B
18. ঝুকিপূর্ণ গর্ভ -> high_risk_pregnancy: (Traduzido do Bengali) Identifica qual o risco da gravidez, o que determina a quantidade de cuidados que serão necessários

## Tratamento dos Dados

### ETL

#### Carregando a Base
df_maternal_health = spark.sql("SELECT * FROM spark_catalog.default.maternal_health")

#### Transformando as colunas
Essas transformações são necessárias para ser possivel trabalhar métricas com soma, média, entre outras.
df_maternal_health = df_maternal_health\
    .withColumn("pregnancy", col("pregnancy").substr(1, 1).cast("int"))\
    .withColumn("weeks", col("weeks").substr(1, 1).cast("int"))\
    .withColumn("weight_kg", regexp_replace(col("weight_kg"), "kg", "").cast("int"))\
    .withColumn("height_ft", regexp_replace(col("height_ft"), "''", ""))\
    .withColumn("height_cm", 
                   format_number(
                       bround(
                           (col("height_ft").substr(1, 1).cast("float") * 30.48) + 
                           (col("height_ft").substr(3, 2).cast("float") * 2.54), 
                           2
                       ).cast("double"), 2))\
    .withColumn("systolic", split(col("blood_pressure"), "/").getItem(0).cast("int"))\
    .withColumn("diastolic", split(col("blood_pressure"), "/").getItem(1).cast("int"))\
    .withColumn("blood_pressure",
            when((col("systolic") < 90) | (col("diastolic") < 60), "1")
            .when(((col("systolic") >= 90) & (col("systolic") <= 120)) & ((col("diastolic") >= 60) & (col("diastolic") <= 80)), "2")
            .otherwise("3")
            .cast("int"))\
    .withColumn("anemia",
                when((col("anemia") == 'Minimal'), "1")
                .when((col("anemia") == 'Medium'), "2")
                .when((col("anemia") == 'Higher'), "3")
                .otherwise("0")
                .cast("int"))\
    .withColumn("jaundice",
                when((col("jaundice") == 'Minimal'), "1")
                .when((col("jaundice") == 'Medium'), "2")
                .when((col("jaundice") == 'Higher'), "3")
                .otherwise("0")
                .cast("int"))\
    .withColumn("fetal_position",
                when((col("fetal_position") == 'Abnormal'), "1")
                .otherwise("0")
                .cast("int"))\
    .withColumn("fetal_movements",
                when((col("fetal_movements") == 'Abnormal'), "1")
                .otherwise("0")
                .cast("int"))\
    .withColumn("fetal_heartbeat", regexp_replace(col("fetal_heartbeat"), "m", "").cast("int"))\
    .withColumn("urine_test_albumin",
                when((col("urine_test_albumin") == 'Minimal'), "1")
                .when((col("urine_test_albumin") == 'Medium'), "2")
                .when((col("urine_test_albumin") == 'Higher'), "3")
                .otherwise("0")
                .cast("int"))\
    .withColumn("urine_test_sugar",
                when((col("urine_test_sugar") == 'Yes'), "1")
                .otherwise("0")
                .cast("int"))\
    .withColumn("vdrl",
                when((col("vdrl") == 'Positive'), "1")
                .otherwise("0")
                .cast("int"))\
    .withColumn("hrsag",
                when((col("hrsag") == 'Positive'), "1")
                .otherwise("0")
                .cast("int"))\
    .withColumn("high_risk_pregnancy",
                when((col("high_risk_pregnancy") == 'Yes'), "1")
                .otherwise("0")
                .cast("int"))

*Tratamento da blood_pressure 
Baixa (1): Sistólica < 90 ou Diastólica < 60
Normal (2): Sistólica entre 90 e 120 e Diastólica entre 60 e 80
Alta (3): Sistólica > 120 ou Diastólica > 80
Fonte: American Heart Association (AHA) e da Organização Mundial da Saúde (OMS).

## Modelagem

### Criando os id
#id_pregnant
window_spec_fato = Window.orderBy("name")
df_maternal_health = df_maternal_health\
    .withColumn("id_pregnant", row_number().over(window_spec_fato))

#id_exames
window_spec_dim_pregnant = Window.orderBy("blood_pressure")
df_maternal_health = df_maternal_health\
    .withColumn("id_exames", row_number().over(window_spec_dim_pregnant))

#id_baby
window_spec_dim_exames = Window.orderBy("fetal_position")
df_maternal_health = df_maternal_health\
    .withColumn("id_baby", row_number().over(window_spec_dim_exames))

#id_pregnancy
window_spec_dim_baby = Window.orderBy("pregnancy")
df_maternal_health = df_maternal_health\
    .withColumn("id_pregnancy", row_number().over(window_spec_dim_baby))

###Criando o modelo Estrela

####df_fato_maternal_health = df_maternal_health.select("id_pregnant", "id_exames", "id_baby", "id_pregnancy")

Descrição das colunas
id_pregnant : Criada após a classificação da coluna "name"
id_exames : Criada após a classificação da coluna "blood_pressure" 
id_baby : Criada após a classificação da coluna "fetal_position"
id_pregnancy : Criada após a classificação da coluna "pregnancy"

####df_dim_pregnant = df_maternal_health.select("id_pregnant", "name", "age", "weight_kg", "height_cm")

Descrição das colunas
id_pregnant : Criada após a classificação da coluna "name"
name : 
age : 
weight_kg : 
height_cm : 

####df_dim_exames = df_maternal_health.select("id_exames", "blood_pressure", "anemia", "jaundice", "urine_test_albumin", "urine_test_sugar", "vdrl", "hrsag")

Descrição das colunas
id_exames : Criada após a classificação da coluna "blood_pressure" 
blood_pressure : 
anemia : 
jaundice : 
urine_test_albumin : 
urine_test_sugar : 
vdrl : 
hrsag : 

####df_dim_baby = df_maternal_health.select("id_baby", "fetal_position", "fetal_movements", "fetal_heartbeat")

Descrição das colunas
id_baby : Criada após a classificação da coluna "fetal_position"
fetal_position : 
fetal_movements : 
fetal_heartbeat : 

####df_dim_pregnancy = df_maternal_health.select("id_pregnancy", "pregnancy", "weeks", "high_risk_pregnancy")

Descrição das colunas
id_pregnancy : Criada após a classificação da coluna "pregnancy"
pregnancy : 
weeks : 
high_risk_pregnancy : 

## Respondendo as perguntas

1. A idade está relacionada a gestações de alto risco?
Para responder essa pergunta vamos criar uma classe para validar a idade e depois ver o % de pacientes por classe:

df_age_risk = df_maternal_health\
    .withColumn("class_age",
            when((col("age") < 18), "- 18")
            .when((col("age") <= 20), "18 - 20")
            .when((col("age") <= 25), "21 - 25")
            .when((col("age") <= 30), "26 - 30")
            .when((col("age") < 50), "31 - 50")
            .otherwise("+ 50"))\

df_age_risk = df_age_risk.groupBy("class_age").agg(
    avg(when(col("high_risk_pregnancy") == 1, 1).otherwise(0)).alias("risk_high"),
    avg(when(col("high_risk_pregnancy") == 0, 1).otherwise(0)).alias("risk_normal")
)

A tabela gerada foi a seguinte:

class_age |	risk_high | risk_normal
18 - 20   | 0.6645	  | 0.33540
21 - 25   | 0.6694	  | 0.3305
26 - 30   | 0.6628	  | 0.3371
31 - 50   | 0.6923	  | 0.3076

Podemos avaliar que a idade não é um fator predominantem para uma gravidez de risco, sendo um pouco mais frequente em pascientes com mais de 31 anos.

2. A quantidade de vezes que a paciente ficou grávida está relacionada a gestações de alto risco?

df_pregnancy_risk = df_maternal_health.groupBy("pregnancy").agg(
    avg(when(col("high_risk_pregnancy") == 1, 1).otherwise(0)).alias("risk_high"),
    avg(when(col("high_risk_pregnancy") == 0, 1).otherwise(0)).alias("risk_normal"))

A tabela gerada foi a seguinte:

pregnancy |	risk_high | risk_normal
1         |	0.6666    |	0.3333
3         |	0.6       | 0.4
2         |	0.6730    |	0.3269

Podemos avaliar que a quantidade de gestações não é um fator predominantem para uma gravidez de risco.

3. O IMC está relacionada a gestações de alto risco?
Para responder essa pergunta vamos cálcular o IMC criando uma classe e depois ver o % de pacientes por classe:

df_imc_risk = df_maternal_health\
    .withColumn("IMC", col("weight_kg") / (col("height_cm") ** 2))\
    .withColumn("class_imc",
        when(col("IMC") < 18.5, "Abaixo do peso")
        .when((col("IMC") >= 18.5) & (col("IMC") < 24.9), "Peso normal")
        .when((col("IMC") >= 25) & (col("IMC") < 29.9), "Sobrepeso")
        .when(col("IMC") >= 30, "Obesidade"))

df_imc_risk = df_imc_risk.groupBy("class_imc").agg(
    avg(when(col("high_risk_pregnancy") == 1, 1).otherwise(0)).alias("risk_high"),
    avg(when(col("high_risk_pregnancy") == 0, 1).otherwise(0)).alias("risk_normal")
)

A tabela gerada foi a seguinte:

class_imc	   | risk_high | risk_normal
Abaixo do peso | 0.66733   | 0.3326

A base utilizada não contempla todos os tipos de IMC, então não é possivel atraves dela definir se o IMC tem relação com o risco da gravidez

4. Existe uma correlação entre a pressão arterial elevada (hipertensão) e a gravidez de alto risco?

df_pressure_risk = df_maternal_health.groupBy("blood_pressure").agg(
    avg(when(col("high_risk_pregnancy") == 1, 1).otherwise(0)).alias("risk_high"),
    avg(when(col("high_risk_pregnancy") == 0, 1).otherwise(0)).alias("risk_normal"))
    
A tabela gerada foi a seguinte:

blood_pressure | risk_high | risk_normal
1              | 0.03149   | 0.9685
2              | 0.76004   | 0.2399

Baixa - 1, Normal - 2, Alta - 3

A base utilizada não contempla todos os tipos de pressão, então não é possivel atraves dela definir se o hipertensão tem relação com o risco da gravidez, uma vez que ela não foi contemplada.

5. Como o histórico de anemia impacta a probabilidade de uma gravidez ser considerada de alto risco?

df_anemia_risk = df_maternal_health.groupBy("anemia").agg(
    avg(when(col("high_risk_pregnancy") == 1, 1).otherwise(0)).alias("risk_high"),
    avg(when(col("high_risk_pregnancy") == 0, 1).otherwise(0)).alias("risk_normal"))
    
A tabela gerada foi a seguinte:

anemia | risk_high | risk_normal
1      | 0.6612    | 0.3387
2      | 0.6721    | 0.3278
0      | 0.6674    | 0.3325

Normal - 0, Minimal - 1, Medium - 2, Higher - 3

Podemos avaliar que a anemia não é um fator predominantem para uma gravidez de risco.

6. Como o histórico de jaundice impacta a probabilidade de uma gravidez ser considerada de alto risco?

df_jaundice_risk = df_maternal_health.groupBy("jaundice").agg(
    avg(when(col("high_risk_pregnancy") == 1, 1).otherwise(0)).alias("risk_high"),
    avg(when(col("high_risk_pregnancy") == 0, 1).otherwise(0)).alias("risk_normal"))
    
A tabela gerada foi a seguinte:

jaundice | risk_high | risk_normal
1	     | 0.625     | 0.375
2	     | 0.75      | 0.25
0	     | 0.6673    | 0.3326

Normal - 0, Minimal - 1, Medium - 2, Higher - 3

Podemos avaliar que a presença acima de média da icteríciapode ser um fator importante para uma gravidez de risco, porém a base não contempla quando o exame apresenta alta presença e com isso não é possivel afirmar.

7. A posição fetal anormal é mais frequentemente em gravidez de alto risco?

df_fetal_risk = df_maternal_health.groupBy("fetal_position").agg(
    avg(when(col("high_risk_pregnancy") == 1, 1).otherwise(0)).alias("risk_high"),
    avg(when(col("high_risk_pregnancy") == 0, 1).otherwise(0)).alias("risk_normal"))
    
A tabela gerada foi a seguinte:

fetal_position | risk_high | risk_normal
1	           | 0.6666    | 0.3333
0	           | 0.6673    | 0.3326

Abnormal  - 1, Normal - 0

Podemos avaliar que a posição fetal anormal não é um fator predominantem para uma gravidez de risco.

8. Como o histórico de urine_test_albumin impacta a probabilidade de uma gravidez ser considerada de alto risco?

df_albumin_risk = df_maternal_health.groupBy("urine_test_albumin").agg(
    avg(when(col("high_risk_pregnancy") == 1, 1).otherwise(0)).alias("risk_high"),
    avg(when(col("high_risk_pregnancy") == 0, 1).otherwise(0)).alias("risk_normal"))
    
A tabela gerada foi a seguinte:

urine_test_albumin | risk_high | risk_normal
1                  | 0.6428    | 0.3571
3                  | 0.6590    | 0.3409
2                  | 0.6666    | 0.3333
0                  | 0.6689    | 0.3310

Podemos avaliar que o histórico de urine_test_albumin não é um fator predominantem para uma gravidez de risco.

9. Como o histórico de urine_test_sugar impacta a probabilidade de uma gravidez ser considerada de alto risco?

df_sugar_risk = df_maternal_health.groupBy("urine_test_sugar").agg(
    avg(when(col("high_risk_pregnancy") == 1, 1).otherwise(0)).alias("risk_high"),
    avg(when(col("high_risk_pregnancy") == 0, 1).otherwise(0)).alias("risk_normal"))
    
A tabela gerada foi a seguinte:

urine_test_sugar | risk_high | risk_normal
1                | 0.6662    | 0.3337
0                | 0.6759    | 0.3240

Podemos avaliar que o histórico de urine_test_sugar não é um fator predominantem para uma gravidez de risco.

10. Como o histórico de vdrl impacta a probabilidade de uma gravidez ser considerada de alto risco?

df_vdrl_risk = df_maternal_health.groupBy("vdrl").agg(
    avg(when(col("high_risk_pregnancy") == 1, 1).otherwise(0)).alias("risk_high"),
    avg(when(col("high_risk_pregnancy") == 0, 1).otherwise(0)).alias("risk_normal"))
    
A tabela gerada foi a seguinte:

vdrl | risk_high | risk_normal
1	 | 0.6673	 | 0.3326
0	 | 0.6673	 | 0.3326

Podemos avaliar que o histórico de vdrl não é um fator predominantem para uma gravidez de risco.

11. Como o histórico de hrsag impacta a probabilidade de uma gravidez ser considerada de alto risco?

df_hrsag_risk = df_maternal_health.groupBy("hrsag").agg(
    avg(when(col("high_risk_pregnancy") == 1, 1).otherwise(0)).alias("risk_high"),
    avg(when(col("high_risk_pregnancy") == 0, 1).otherwise(0)).alias("risk_normal"))
    
A tabela gerada foi a seguinte:

hrsag | risk_high | risk_normal
1     | 1	      | 0
0     | 0.6265    | 0.3734

Podemos avaliar que todas as pacientes que possuem histórico de hrsag é um fator predominantem para uma gravidez de risco, onde 100% dos casos que possuem histórico de hrsag são de gravidez de risco.

## Qualidade dos dados

A base de dados escolhida já estava devidamente curada e tratada. Além disso, realizei transformações para eliminar valores nulos e padronizar as informações, garantindo que fosse possível a criação de métricas confiáveis.
No entanto, um problema identificado é que a base apresenta uma falta de diversidade nos dados. Por exemplo:
- Nenhuma das pacientes apresenta histórico de pressão alta, o que é amplamente reconhecido na comunidade médica como um fator que aumenta o risco durante a gravidez, especialmente no desenvolvimento de pré-eclâmpsia. Fonte: American Heart Association (AHA) e Organização Mundial da Saúde (OMS).
- Todas as pacientes estão abaixo do peso.
- A maioria dos dados são muito parecidos para pacientes com e sem risco.

## Autoavalização

A escolha desta base de dados se deu pelo fato de eu estar grávida de gêmeos, o que é considerado uma gravidez de risco. No entanto, infelizmente, não encontrei nenhuma base com informações específicas sobre gravidez gemelar, mas encontrei esta, que achei interessante.
O meu objetivo principal era identificar quais métricas uma gestante deve monitorar para se preparar adequadamente para uma possível gravidez de risco. Embora uma gravidez de risco habitual possa ser tranquila, é importante destacar que uma gravidez com risco elevado exige uma maior assistência e apoio especializado.
Acredito que essa base, por si só, não foi suficiente para atingir esse objetivo, principalmente devido à falta de dados mais diversificados, como mencionei anteriormente. Mas atravez dela podemos dizer que 
