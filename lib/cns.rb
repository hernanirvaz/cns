# frozen_string_literal: true

require('thor')
require('cns/apibc')
require('cns/apice')
require('cns/bigquery')
require('cns/etherscan')
require('cns/greymass')
require('cns/beaconchain')
require('cns/bitcoinde')
require('cns/kraken')
require('cns/paymium')
require('cns/therock')
require('cns/version')

module Cns
  # classe para carregar/mostrar dados transacoes eth & eos no bigquery
  class CLI < Thor
    desc 'weth', 'carrega transacoes eth no bigquery'
    option :h, type: :hash, default: {}, desc: 'configuracao ajuste reposicionamento temporal'
    # carrega transacoes eth no bigquery
    def weth
      Bigquery.new(options).processa_weth
    end

    desc 'work', 'carrega transacoes novas no bigquery'
    option :h, type: :hash, default: {}, desc: 'configuracao ajuste reposicionamento temporal'
    # carrega transacoes novas no bigquery
    def work
      Bigquery.new(options).processa_tudo
    end

    desc 'show', 'mostra resumo transacoes'
    option :v, type: :boolean, default: false, desc: 'mostra transacoes'
    option :t, type: :boolean, default: false, desc: 'mostra transacoes todas ou somente novas'
    # mostra resumo transacoes
    def show
      Bigquery.new(options).mostra_tudo
    end

    default_task :show
  end
end
