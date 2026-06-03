# Aula JPA Maven 2

Projeto Java desenvolvido para praticar os conceitos básicos de JPA com Hibernate e Maven.

A aplicação demonstra como configurar uma unidade de persistência com persistence.xml, 
mapear uma entidade Java com anotações JPA e realizar operações de persistência em um banco de dados MySQL.

## Tecnologias utilizadas

- Java 21
- Maven
- Jakarta Persistence API
- Hibernate ORM
- MySQL

## Funcionalidades

- Configuração de JPA com persistence.xml
- Mapeamento da entidade Pessoa
- Criação automática/atualização da tabela no banco com Hibernate
- Persistência de objetos no banco de dados usando EntityManager
- Transações com RESOURCE_LOCAL

## Estrutura principal

- dominio.Pessoa: entidade JPA que representa uma pessoa com id, nome e email
- aplicacao.Programa: classe principal que cria objetos Pessoa e salva no banco
- META-INF/persistence.xml: arquivo de configuração da conexão com o MySQL e do Hibernate

## Banco de dados

O projeto utiliza o banco MySQL com a base

