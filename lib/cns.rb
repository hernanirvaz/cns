# frozen_string_literal: true

require('thor')
require('cns/version')

module Cns
  # classe para erros desta gem
  class Erro < StandardError
    # @return [StandardError] personalizacao dos erros
    def initialize(msg)
      super(msg)
    end
  end

  # classe para carregar/mostrar dados transacoes eth & eos no bigquery
  class CLI < Thor
    desc 'work', 'carrega transacoes novas no bigquery'
    option :h, type: :hash, default: {}, desc: 'configuracao ajuste reposicionamento temporal'
    # carrega transacoes novas no bigquery
    def work
      p('Bct::Bigquery.new(options).processa_tudo')
    end

    desc 'show', 'mostra resumo transacoes'
    option :v, type: :boolean, default: false, desc: 'mostra transacoes'
    option :t, type: :boolean, default: false, desc: 'mostra transacoes todas ou somente novas'
    # mostra resumo transacoes
    def show
      p('Bct::Bigquery.new(options).mostra_tudo')
    end

    default_task :show
  end
end
