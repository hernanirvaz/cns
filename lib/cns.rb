# frozen_string_literal: true

require('thor')
require('cns/apibc')
require('cns/apice1')
require('cns/apice2')
require('cns/bigquery1')
require('cns/bigquery2')
require('cns/bigquery3')
require('cns/bigquery4')
require('cns/etherscan1')
require('cns/etherscan2')
require('cns/greymass1')
require('cns/greymass2')
require('cns/beaconchain1')
require('cns/beaconchain2')
require('cns/bitcoinde')
require('cns/kraken')
require('cns/paymium')
require('cns/therock')
require('cns/version')

module Cns
  # classe para carregar/mostrar dados transacoes eth & eos no bigquery
  class CLI < Thor
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
