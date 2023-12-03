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
    desc 'seth', 'mostra eth transacoes'
    option :v, type: :boolean, default: true, desc: 'mostra transacoes'
    option :t, type: :boolean, default: false, desc: 'mostra transacoes todas ou somente novas'
    # mostra eth transacoes
    def seth
      Bigquery.new(options).mostra_seth
    end

    desc 'weth', 'carrega transacoes eth no bigquery'
    option :h, type: :hash, default: {}, desc: 'configuracao ajuste reposicionamento temporal'
    # carrega transacoes eth no bigquery
    def weth
      Bigquery.new(options).processa_weth
    end

    desc 'ceth', 'carrega transacoes eth no bigquery (cron)'
    option :h, type: :hash, default: {}, desc: 'configuracao ajuste reposicionamento temporal'
    # carrega transacoes eth no bigquery (output to file)
    def ceth
      Bigquery.new(options).processa_ceth
    end

    desc 'skrk', 'mostra kraken/eth transacoes'
    option :v, type: :boolean, default: true, desc: 'mostra transacoes'
    option :t, type: :boolean, default: false, desc: 'mostra transacoes todas ou somente novas'
    # mostra kraken/eth transacoes
    def skrk
      Bigquery.new(options).mostra_skrk
    end

    desc 'wkrk', 'carrega transacoes kraken/eth no bigquery'
    option :h, type: :hash, default: {}, desc: 'configuracao ajuste reposicionamento temporal'
    # carrega transacoes kraken/eth no bigquery
    def wkrk
      Bigquery.new(options).processa_wkrk
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

    default_task :seth
  end
end
