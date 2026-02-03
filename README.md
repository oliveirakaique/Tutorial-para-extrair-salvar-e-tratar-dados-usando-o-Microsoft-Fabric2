
# Tutorial para extrair salvar e tratar dados usando o Microsoft Fabric
Tratar dados de forma eficiente não é só sobre tecnologia, é sobre evitar retrabalho, reduzir custo e ganhar tempo lá na frente. Quem já montou pipeline do zero sabe: se você não pensar em escalabilidade e reaproveitamento desde o início, o projeto vira um Frankenstein bem rápido.

Neste artigo, apresento uma solução para extração, armazenamento e tratamento de dados, utilizando uma arquitetura medalhão **raw [bronze]**, **silver [prata]** e **gold [ouro]**, totalmente automatizada e otimizada.
<br>

### Pipeline e notebooks
<img width="917" height="648" alt="image" src="https://github.com/user-attachments/assets/df1b2939-f2d2-41f0-aa91-17c6b6921d7e" /><br>

#### 1. Configuração inicial dos notebooks no pipeline `pl_dados_mdic`

O primeiro passo consiste em configurar os notebooks no pipeline `pl_dados_mdic`. Para isso, utilizamos [parâmetros no pipeline](https://learn.microsoft.com/pt-br/fabric/data-factory/parameters#creating-and-using-parameters), permitindo definir dinamicamente os nomes dos lakehouses e dos caminhos de armazenamento de dados.

Para reutilizar o pacote `pl_dados_mdic.zip`, é necessário informar:

* Nome do lakehouse da camada **raw**
* Nome do lakehouse da camada **silver**
* [Caminho ABFSS](https://learn.microsoft.com/pt-br/fabric/data-engineering/lakehouse-notebook-load-data#load-data-with-an-apache-spark-api) da pasta `Files` da camada **raw**

Os dados são disponibilizados pelo portal do [Ministério do Desenvolvimento, Indústria, Comércio e Serviços (MDIC)](https://www.gov.br/mdic/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta), especificamente na base detalhada por município da empresa exportadora/importadora e por posição do Sistema Harmonizado (SH4).

Os arquivos originais são fornecidos em formato CSV. No pipeline, eles são convertidos para o formato **Parquet** e armazenados na pasta `Files`, o que proporciona:

* Melhor desempenho de leitura e processamento
* Redução de espaço em disco
* Padronização do armazenamento analítico

#### 2. Noteboos `nb_raw_mdic_fatos` e `nb_raw_mdic_dim`

No notebook `nb_raw_mdic_fatos` consiste na extração dos dados brutos, persistindo os arquivos na pasta `Files` do lakehouse da camada **raw**. No portal, os dados estão compilados e separados por ano. Os arquivos podem ser acessados por meio de links externos `https://balanca.economia.gov.br/balanca/bd/comexstat-bd/mun/`, cujo final da URL varia conforme o ano e o tipo de estatística, por exemplo: `EXP_2025_MUN.csv` e `IMP_2025_MUN.csv`.

<img width="1909" height="835" alt="image" src="https://github.com/user-attachments/assets/ae2559ad-5b4d-4421-af52-bc9360c27446" /><br>

O notebook `nb_raw_mdic_dim` consiste na extração das tabelas de correlação de códigos e classificações, que compõem as dimensões do modelo de dados. A base da URL é `https://balanca.economia.gov.br/balanca/bd/tabelas/`, variando conforme a tabela selecionada na extração, por exemplo: `NCM.csv` e `NCM_CUCI.csv`.

<img width="1824" height="870" alt="image" src="https://github.com/user-attachments/assets/d8aad563-cb3f-41bd-909c-10ad8511a144" /><br>

#### 3. Noteboos `nb_raw_mdic_etl_fatos` e `nb_raw_mdic_etl_dim`
O notebook `nb_raw_mdic_etl_fatos` realiza a transformação dos arquivos Parquet de importação e exportação em duas tabelas Delta, que são persistidas na camada **raw**. Antes da gravação dos dados, é utilizado o parâmetro [`checkpointLocation`](https://learn.microsoft.com/pt-br/azure/databricks/structured-streaming/checkpoints), que define o diretório onde o Spark armazena o estado do processamento, garantindo tolerância a falhas, consistência dos dados e evitando o reprocessamento de arquivos já ingeridos. O parâmetro `checkpointLocation` também atua na consolidação dos logs de transação do Delta Lake. Em cargas frequentes, cada operação gera um novo arquivo de log, o que pode degradar a performance de leitura ao longo do tempo. Periodicamente, esses logs são consolidados em arquivos de checkpoint, que resumem o histórico de transações. Dessa forma, ao consultar a tabela, o sistema lê apenas o checkpoint mais recente e poucos logs adicionais, melhorando significativamente a eficiência e o desempenho. Neste caso, ele irá reprocessar apenas os novos arquivos salvos na pasta `Files`.

Por sua vez, o notebook `nb_raw_mdic_etl_dim` define quais arquivos serão transformados em tabelas Delta. Como ponto de atenção, foi implementada a verificação `spark.catalog.tableExists`, que valida se determinada tabela já existe no catálogo. Caso exista, o processamento é ignorado, evitando reprocessamento desnecessário. Essa abordagem é adotada porque as dimensões fornecidas pelo MDIC são completas e estáveis. Eventualmente, podem surgir novos produtos na balança comercial que ainda não estejam cadastrados nas tabelas de detalhamento; nesse caso, basta remover a condição `spark.catalog.tableExists` para forçar o reprocessamento das dimensões.

<img width="1427" height="856" alt="image" src="https://github.com/user-attachments/assets/7ac1d141-5465-477d-9523-544fe9dffc73" /><br>

#### 4. Noteboos `nb_silver_mdic_etl`
Nesta etapa, é realizado o ETL das tabelas Delta armazenadas na camada **raw**, onde são definidos os nomes das colunas, realizados os relacionamentos entre tabelas correlatas e ajustados os tipos de dados. Em essência, os dados são promovidos da camada **raw** para a **silver** com pequenas transformações e padronizações.
























































### Camada Raw [Raw]

A Camada Bronze é o primeiro nível na Arquitetura Medallion (Medallion Architecture) de gerenciamento de dados, focada na ingestão e armazenamento de dados brutos (raw data) provenientes de diversas fontes (APIs, bancos de dados, logs). 
Nele iremos ter duas pastas para armazenar dados que são dimensões e fatos. Os dados podem ser baixos pelo portal do [MDIC](https://www.gov.br/mdic/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta).
























































### Configuração do pipeline de dados para atualização automática dos dados
1. No workspace, importe os notebooks anexados neste tutorial. Neles estarão os códigos apresentados acima.  Você pode importar um ou mais notebooks existentes do computador local usando a barra de ferramentas do workspace.  Leia a documentação sobre como importar notebooks [aqui](https://learn.microsoft.com/pt-br/fabric/data-engineering/how-to-use-notebook#import-existing-notebooks).

2. Crie três lakehouses com os seguintes nomes: **lk_raw_dados_publicos**, **lk_silver_dados_publicos** e **lk_gold_dados_publicos**. Obs: Não estou definido esquemas no lakehouse. 
   Esses lakehouses irão armazenar os dados de importações e exportações brasileiras. O nome **dados_publicos** é apenas sugestivo e segue a recomendação de uso para dados de origem pública, como índices de preços, indicadores de atividade econômica e similares. Neste tutorial, os dados tratados são do MDIC. Veja como criar um [lakehouse](https://learn.microsoft.com/pt-br/fabric/data-engineering/create-lakehouse).

3. Crie um novo item na página inicial do workspace onde serão armazenados os notebooks e pipelines. Leia a documentação sobre como criar um [pipeline](https://learn.microsoft.com/pt-br/fabric/data-factory/pipeline-landing-page).

4. Configure as atividades do pipeline para executar os notebooks na ordem:
   raw → silver → gold, garantindo a atualização automática dos dados. Para configurar o pipeline basta importar o json para o item pipeline que criou. Isso é possível pois alguns itens do Microoft Fabric são armazenados em formato json, contendo todas as configurações naquele item em questão. Desta forma você consegue configurá-lo como foi feito neste projeto.

5. Após concluir essa configuração, será necessário ajustar os parâmetros definidos nos notebooks. Cada notebook permite a parametrização interna, e o bloco que contém esses parâmetros pode ser substituído por parâmetros definidos externamente (por exemplo, pelo pipeline). Neste projeto, são utilizados três parâmetros: os nomes dos lakehouses criados e o caminho absoluto do sistema de arquivos do Azure Blob Storage (ABFS). Nesse caso, informe o caminho ABFS correspondente à pasta **Files** do lakehouse **lk_raw_dados_publicos**. Veja como carregar dados em uma pasta ABFS: [ABFS](https://learn.microsoft.com/pt-br/fabric/data-engineering/lakehouse-notebook-load-data#load-data-with-an-apache-spark-api)

<img width="1468" height="298" alt="image" src="https://github.com/user-attachments/assets/304f30cd-5ca9-4aeb-9c28-e6e9ee246327" />

### Configuração do pipeline cadeia de atualização dos dados explicado:
Versão mais clara, direta e técnica:

1. **Get Metadata**: verifica se existe a pasta no lakehouse bronze onde serão armazenados os arquivos Parquet.

2. **Condição IF**: avalia o campo `exists` retornado pelo Get Metadata.

   * Se o valor for **false**, significa que o pipeline/notebooks ainda não foram executados e não há arquivos na pasta **Files**. Nesse caso, o pipeline define o ano inicial de extração como **1997**.
   * Se o valor for **true**, indica que já existem arquivos armazenados. Assim, o pipeline extrai os dados a partir do **ano anterior** até o **ano atual**.

Essa lógica pode ser ajustada conforme a necessidade. A estrutura com **Get Metadata + IF Condition** foi criada apenas para permitir a carga inicial automática na primeira execução.

3. **Variável data fim**: foi definida como o ano atual, indicando que a extração dos dados deve ocorrer até o período mais recente disponível (ano corrente).

4. Os notebooks **nb_raw_mdic_fatos** e **nb_raw_mdic_dim** são responsáveis por extrair os arquivos e salvá-los na pasta **Files** da camada bronze. Em seguida, os notebooks **nb_raw_mdic_etl_ft** e **nb_raw_mdic_etl_dim** executam o processo de ETL sobre os dados brutos, transformando os arquivos Parquet armazenados em **Files** e salvando-as em **tabelas Delta** camada bronze. 

5. Um **Wait de 10 segundos** é aplicado antes de disparar a execução do notebook **nb_silver_mdic_etl**.

Texto claro e corrigido:

6. O notebook **nb_silver_mdic_etl** é responsável por processar os dados brutos da camada bronze, realizando a normalização das tabelas, joins, tipagem de dados e a consolidação das tabelas de fatos e dimensões.

**Obs.:** Nas execuções seguintes, algumas tabelas de dimensão não são reprocessadas, pois são dados já bem definidos e consolidados. Para isso, é aplicada a regra `spark.catalog.tableExists`, que verifica se a tabela já existe e evita o reprocessamento desnecessário.

### Camada Gold [Ouro]

Nessa camada, não foi necessário realizar refinamentos ou agregações, pois os dados fornecidos pelo MDIC já estão bem organizados e padronizados. Como os dados já estão tratados na camada silver, basta replicá-los para a camada gold, que é onde o usuário final consome as informações.  

Não é necessário copiar novamente os dados fisicamente; é possível utilizar **shortcuts do OneLake**. Dessa forma, eliminam-se duplicações de dados, reduzindo a latência do processo associada à cópia e ao preparo das informações. Leia a documentação sobre [shortcuts](https://learn.microsoft.com/pt-br/fabric/onelake/onelake-shortcuts#what-are-shortcuts)

### Organização dos arquivos
Abaixo estão os arquivos deste projeto organizados em uma estrutura de pastas. Manter essa organização é fundamental para a sustentação, manutenção e evolução das atividades.

<img width="1012" height="639" alt="image" src="https://github.com/user-attachments/assets/487bf74b-6b04-4987-8f17-3d19b579c3df" />

<img width="476" height="794" alt="image" src="https://github.com/user-attachments/assets/447309d2-aef8-4bd8-9193-584e1ab4c38d" />

<img width="802" height="500" alt="image" src="https://github.com/user-attachments/assets/41e9eadd-2814-4002-9880-a017f3568ac4" />

https://api-comexstat.mdic.gov.br/general/dates/updated

<img width="810" height="464" alt="image" src="https://github.com/user-attachments/assets/10f3f6a2-9ec7-4826-934e-e35e879213a5" />

<img width="481" height="803" alt="image" src="https://github.com/user-attachments/assets/2eb47033-c725-441d-96be-a7bfe1e63bbe" />







