
# Tutorial para extrair salvar e tratar dados usando o Microsoft Fabric
Tratar dados de forma eficiente não é só sobre tecnologia, é sobre evitar retrabalho, reduzir custo e ganhar tempo lá na frente. Quem já montou pipeline do zero sabe: se você não pensar em escalabilidade e reaproveitamento desde o início, o projeto vira um Frankenstein bem rápido.

Neste artigo, apresento uma solução para extração, armazenamento e tratamento de dados, utilizando uma arquitetura medalhão **raw [bronze]**, **silver [prata]** e **gold [ouro]**, totalmente automatizada e otimizada.
<br>

### Lakehouses
Crie três lakehouses com os seguintes nomes: `lk_raw_dados_publicos`, `lk_silver_dados_publicos` e `lk_gold_dados_publicos`.

Obs.: Não é necessário definir esquemas no lakehouse (opcional). Esses lakehouses irão armazenar os dados de importações e exportações brasileiras. O nome **dados_publicos** é apenas sugestivo e segue a recomendação de nomenclatura para dados de origem pública, como índices de preços, indicadores de atividade econômica e similares. Neste tutorial, os dados utilizados são do MDIC.

Veja como criar um [lakehouse](https://learn.microsoft.com/pt-br/fabric/data-engineering/create-lakehouse).
   
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

### Camada Gold

Nessa camada, não foi necessário realizar refinamentos ou agregações, pois os dados fornecidos pelo MDIC já estão bem organizados e padronizados. Como os dados já estão tratados na camada silver, basta replicá-los para a camada gold, que é onde o usuário final consome as informações.  

Não é necessário copiar novamente os dados fisicamente; é possível utilizar **shortcuts do OneLake**. Dessa forma, eliminam-se duplicações de dados, reduzindo a latência do processo associada à cópia e ao preparo das informações. Leia a documentação sobre [shortcuts](https://learn.microsoft.com/pt-br/fabric/onelake/onelake-shortcuts#what-are-shortcuts).

### Pipeline de atualização incremental de dados `pl_dados_mdic_incremental`
Após compreender o funcionamento do pipeline `pl_dados_mdic` e sua configuração, é necessário definir as cargas **full** e **incrementais**. Na carga full, os dados são extraídos desde `01-01-1997` até o ano corrente. Em seguida, o pipeline passa a operar de forma incremental, processando apenas os novos dados publicados. Para entender como realizar cargas incrementais de dados, consulte o tutorial oficial: [leia](https://learn.microsoft.com/pt-br/fabric/data-factory/tutorial-incremental-copy-data-warehouse-lakehouse).

A estratégia de watermark (marca d’água) na engenharia de dados da Microsoft consiste em rastrear e processar de forma eficiente alterações incrementais nos pipelines, evitando o reprocessamento completo dos dados de origem. No nosso caso, o pipeline de ingestão `pl_dados_mdic` será acionado apenas quando houver a publicação de novos dados. Para isso, utilizamos o endpoint [`api-comexstat.mdic`](https://api-comexstat.mdic.gov.br/docs#/paths/general-dates-updated/get), que informa a última atualização disponível para consulta. Quando o Ministério publicar novos dados, o pipeline compara o output da API com a data armazenada no arquivo de watermark criado no ambiente.

### Configuração da atualização incremental
No lakehouse `lk_raw_dados_publicos`, serão criadas duas estruturas de pastas: a pasta `api_comexstat_mdic`, contendo as subpastas `data` e `json`, e uma pasta geral em `Files` para armazenamento dos demais arquivos. Todos os arquivos estão em anexo. Antes de disparar a atualização do pipeline `pl_dados_mdic_incremental`, é necessário criar as pastas e importar os respectivos arquivos, pois eles são a base de controle para o processamento incremental.<br>

<img width="1501" height="395" alt="image" src="https://github.com/user-attachments/assets/dfc77b34-d8db-408f-8998-49fc4d2cdaab" /><br>

* Na pasta `api_comexstat_mdic` será armazenado o arquivo `watermark.csv`, contendo uma data de referência inicial.
* Na pasta `json` será armazenado o output da API `https://api-comexstat.mdic.gov.br/general/dates/updated`. Ele será salvo automaticamente.
```Python
{
  "data": {
    "updated": "2026-01-06",
    "year": "2025",
    "monthNumber": "12"
  },
  "success": true,
  "message": null,
  "processo_info": null,
  "language": "pt"
}
```
* Na pasta `geral` precisamos acrescentar o arquivo `watermark_templete.csv`

### Funcionamento do pipeline `pl_dados_mdic_incremental`
Este pipeline configura os pontos discutidos anteriormente. Para fins de estudo, ele foi desenvolvido com condições que permitem a reutilização por diferentes usuários que desejem testá-lo, não sendo obrigatório mantê-lo exatamente nesse formato. A seguir, será explicado cada tópico enumerado do pipeline.

Antes de tudo, é importante ressaltar que este é o pipeline principal, responsável por invocar o pipeline `pl_dados_mdic`. Para mais detalhes, consulte a atividade de [invocação de pipeline](https://learn.microsoft.com/pt-br/fabric/data-factory/invoke-pipeline-activity).<br>

<img width="1807" height="847" alt="image" src="https://github.com/user-attachments/assets/e3e1a97c-61f6-48c3-ab4d-4fdab8de6965" />

1. Na parametrização: informe o nome do lakehouse **raw** e **silver**, conforme os nomes sugeridos acima. Em seguida, defina o caminho **ABFSS** da pasta `Files` do lakehouse **raw**.

2. Get Metada: verificará a existência do arquivo json da `api-comexstat.mdic`.

3. Condição **if**: na primeira execução, o resultado será **false**, pois o arquivo JSON ainda não estará armazenado na pasta. Nesse caso, será realizada uma carga **full** dos dados, conforme as operações descritas acima, e o JSON será salvo no diretório. Nas execuções seguintes, quando o arquivo já existir, o resultado será **true**, acionando a atualização do JSON e a comparação com a data registrada no arquivo `watermark.csv` salvo na pasta `api_comexstat_mdic`.

4. Lookup: aqui está o cerne da atualização incremental. Nesta etapa, a data retornada pela API é extraída e comparada com a data do arquivo `watermark.csv`, por meio de lookups nos respectivos arquivos, sendo ambas armazenadas em variáveis para controle do processamento.

5. Condição **If**: se as variáveis apresentarem datas iguais, significa que não houve publicação de novos dados. Nesse caso, o resultado **true** não executa nenhuma ação, apenas encerra o pipeline por meio do `Wait2`. Caso contrário (**false**), o pipeline `pl_dados_mdic` é acionado e o arquivo `watermark.csv` é atualizado com a nova data de referência. Garatimos dessa forma que o pipeline atualizará apenas uma vez por fez.
