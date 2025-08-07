# Aplicativo de Investimentos

#### Aluno: [Leandro Fragoso](https://github.com/LecoFrag/App_Investimentos)
#### Orientadora: [Manoela Kohler]

---

Trabalho apresentado ao curso [BI MASTER](https://ica.puc-rio.ai/bi-master) como pré-requisito para conclusão de curso e obtenção de crédito na disciplina "Projetos de Sistemas Inteligentes de Apoio à Decisão".

---

### Resumo

Este projeto apresenta uma solução completa para análise e gestão de investimentos no mercado de ações brasileiro (B3). Utilizando Python para a coleta, armazenamento e processamento de dados, o projeto integra bibliotecas como yfinance para dados históricos e fundamentais, sqlite3 para persistência de dados em um banco de dados local e streamlit para a criação de um painel interativo. A aplicação permite aos usuários visualizar o desempenho de ativos individuais, gerenciar carteiras de investimento e aplicar estratégias de otimização de portfólio, como a Hierarchical Risk Parity (HRP), auxiliando na tomada de decisão.

A arquitetura do sistema é dividida em dois componentes principais: um script Python de atualização (Atualiza_Banco_Dados.py) que se conecta a APIs de mercado financeiro e a um banco de dados local para manter as informações sempre atualizadas, e um aplicativo web interativo (Streamlit_App_Investimentos.py) que serve como interface do usuário para explorar os dados e executar análises complexas, incluindo backtesting de estratégias e detecção de rompimentos técnicos.

### 1. Introdução

O mercado de investimentos atualmente oferece inúmeras ferramentas para acompanhamento da performance dos seus ativos, assim como o desenvolvimento de carteiras personalizadas. Entretanto, além das opções existentes possuirem limitações, a grande maioria delas solicita o pagamento de mensalidades para utilizar funcionalidades avançadas. Esse trabalho foi desenvolvido com o objetivo de replicar algumas dessas funcionalidades e expandir outras, aplicando conhecimentos que foram adquiridos no MBA BI Master.

O projeto aborda a necessidade de uma análise mais profunda do que a oferecida por plataformas financeiras gratuitas, permitindo que investidores possam:

- Acompanhar o desempenho histórico e atual das ações listadas na B3.
- Analisar indicadores fundamentais e técnicos para uma avaliação multidimensional dos ativos.
- Criar e gerenciar carteiras de investimento personalizadas, com a capacidade de importar dados de planilhas e monitorar a performance ao longo do tempo.
- Explorar métodos avançados de alocação de ativos, como a otimização de portfólio baseada em Hierarchical Risk Parity (HRP), para construir carteiras mais resilientes e diversificadas.
- Executar backtesting para comparar o desempenho de diferentes estratégias de alocação em períodos históricos, contra benchmarks como CDI, IPCA e IBOV.

### 2. Modelagem

A modelagem de dados e a arquitetura do sistema foram projetadas para serem eficientes e escaláveis. 

a. Arquivos do Projeto:
A solução conta com alguns arquivos que serão necessários para a inicialização e funcionamento correto do aplicativo. Todos eles devem ser colocados na mesma pasta antes de rodar pela primeira vez o "Atualiza_Banco_Dados.py".
- Atualiza_Banco_Dados.py: Este é o script principal para a coleta e o armazenamento dos dados. Ele se conecta a APIs de mercado financeiro e a um banco de dados local para manter as informações atualizadas.
- Streamlit_App_Investimentos.py: Este é o aplicativo web interativo que serve como interface do usuário para explorar os dados e executar análises complexas.
- dados_investimentos.db: Este é o banco de dados SQLite que serve como a principal fonte de dados para o projeto, e é gerado automaticamente na primeira vez que o script "Atualiza_Banco_Dados.py" é rodado. Ele armazena dados históricos de ações, dados fundamentais de empresas, e indicadores macroeconômicos como CDI, IPCA e IBOV.
- acoes_listadas_b3.csv: Um arquivo CSV que contém a lista de tickers de ações negociadas na B3. Este arquivo é usado pelo script de atualização para saber quais ativos buscar.
- dados_fundamentais_b3.csv: Um arquivo CSV que contém dados fundamentais das empresas listadas. Este arquivo é importado para a tabela dados_fundamentais no banco de dados.
- Carteiras.xlsx: Esse não é um arquivo necessário para rodar o aplicativo, mas ele representa a forma mais prática de subir uma carteira personalizada na plataforma. Já há um exemplo de ativos preenchido.

b. Coleta e Armazenamento de Dados:
O banco de dados SQLite (dados_investimentos.db) é o coração do sistema, servindo como a principal fonte de dados. Tabelas são criadas para armazenar diferentes tipos de informações:
- dados_historicos: Preços de fechamento, volumes, dividendos e outros dados diários para ações.
- dados_fundamentais: Informações detalhadas sobre a empresa, como setor, receita, P/L, ROE, etc.
- dados_indicadores: Taxas de referência macroeconômicas diárias (CDI, IPCA, IBOV) para comparação de performance.
- carteiras: Detalhes sobre os ativos que compõem as carteiras personalizadas do usuário.
O script Atualiza_Banco_Dados.py, quando rodado pela primeira vez, cria a base de dados junto com a estruturação das suas tabelas. Em seguida, faz o download dos dados dos ativos da b3, utilizando a biblioteca yfinance para dados de mercado e requests para buscar indicadores do Banco Central do Brasil (BCB). Após a primeira atualização, as subsequentes buscarão apenas os novos dados, atualizando todas as tabelas necessárias.


c. Modelos de Análise e Otimização:
- Análise de Carteira: A performance da carteira é calculada somando-se o valor de mercado de cada ativo diariamente. O retorno é comparado ao CDI, IPCA e IBOV. Métricas de risco como o VaR (Value at Risk) são calculadas para a carteira agregada.
- Hierarchical Risk Parity (HRP): Um modelo avançado de alocação de portfólio é implementado usando a biblioteca pypfopt. O HRP é utilizado para agrupar ativos com base em suas correlações e alocar pesos que equalizam o risco, oferecendo uma alternativa à otimização tradicional de média-variância, que pode ser sensível a erros de estimativa de parâmetros.
- Backtesting de Estratégias: O sistema simula o desempenho de diferentes estratégias de alocação (HRP, Volatilidade Mínima, Equal Weight) ao longo de um período histórico definido pelo usuário. Ele considera aportes iniciais e mensais para replicar um cenário de investimento contínuo.
- Análise Técnica: O recurso de detecção de rompimentos técnicos identifica ativos cujos preços de fechamento superam ou ficam abaixo da banda de dois desvios-padrão de uma média móvel, fornecendo um sinal visual para potenciais oportunidades de compra ou venda.


### 3. Resultados

Para vizualização de todas as informações foi utilizado o Streamlit, framework de código aberto para Python. O resultado foi uma aplicação funcional e interativa que proporciona uma série de ferramentas para análise de ativos e carteiras pré-determinadas. Nele, foram criadas diversas abas, cada uma com a sua funcionalidade:

- Visão por Ativo: Um painel detalhado para cada ativo, exibindo sua cotação atual, variação percentual em diferentes períodos e um gráfico interativo com linha de tendência e limites de desvio padrão.
- Visão Geral: Aqui é mostrado o desempenho por setor e por ativo dentro do período de 30 dias e 1 ano. Também é calculado o Índice de Mayer por ativo, que divide o preço atual do ativo pela sua média móvel no último ano. Um valor muito acima de 1, por exemplo, mostra que o ativo está acima da média do período, o que pode significar que o ativo está caro (outras análises são necessárias para tirar qualquer conclusão).
- Análise Carteira: A funcionalidade de análise de carteira oferece uma visualização clara do desempenho acumulado, comparando-o com os principais benchmarks do mercado. Gráficos de pizza exibem a distribuição da carteira por setor, auxiliando na identificação de concentrações de risco.
- Otimizador Avançado (HRP): O otimizador de portfólio HRP gera uma sugestão de alocação de um novo aporte, visando equilibrar o risco da carteira. Os resultados são apresentados em tabelas e gráficos que mostram a alocação atual vs. a alocação ideal.
- Backtesting de Estratégias: A ferramenta de backtesting permite aos usuários comparar visualmente o desempenho de estratégias de alocação em um período definido. Uma tabela de resumo mostra o valor final da carteira e o rendimento total, facilitando a escolha da melhor abordagem para cada perfil de investidor.
- Detecção de Rompimentos Técnicos: Essa aba realiza a "análise técnica" explicada na sessão de "2. Modelagem". Há um slider que possibilita aumentar o tamanho da janela da média móvel e uma coluna de "Sinal", que diz se devemos "Aguardar" ou "Analisar compra" de um determinado ativo.
- Carteira de Investimentos: Aba que possibilita a criação de carteiras personalizadas. Há a possibilidade dessa criação por meio do próprio aplicativo ou mesmo fazendo o upload de uma planilha em excel com o ativos, data da compra e quantidade.

Automação: O script de atualização de dados garante que a aplicação sempre funcione com as informações mais recentes, sem a necessidade de intervenção manual para a coleta de dados.


### 4. Conclusões

Este projeto mostra que a maior parte das informações do mercado financiero está, atualmente, disponível de forma gratuita. Combinando a robustez das bibliotecas de análise de dados com a interatividade de uma aplicação web, foi possível criar uma ferramenta poderosa e acessível para investidores. O sistema se destaca por sua capacidade de realizar análises complexas, como a otimização HRP e backtesting, que geralmente estão disponíveis apenas em plataformas profissionais.

As principais conclusões são:

- Automação: A automação da coleta e do armazenamento de dados é fundamental para a viabilidade de uma plataforma de análise de investimentos.
- Otimização de Portfólio: Embora modelos avançados como o HRP ofereçam uma abordagem sofisticada para a diversificação e o controle de risco, ela por si só não pode ser considerada como única fonte de informações. Por isso, outras formas de análise foram inseridas, inclusive o Backtesting;
- Valor do Backtesting: A capacidade de simular e comparar diferentes estratégias em dados históricos é uma maneira poderosa de validar hipóteses de investimento e construir confiança nas decisões. Em alguns casos, a estratégia HRP se mostrou inferior a outras tradicionais ou mesmo indicadores como CDI e IBOVESPA.

O trabalho conclui que esta aplicação atende perfeitamente a necessidade de uma ferramenta gratuita de gestão de investimentos, assim como se mostra uma prova de conceito robusta para a aplicação de princípios de Ciência de Dados em uma situação real.

---

Matrícula: 231.101.014

Pontifícia Universidade Católica do Rio de Janeiro


Curso de Pós Graduação *Business Intelligence Master*


