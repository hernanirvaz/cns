# frozen_string_literal: true

require('faraday')
require('json')

# @author Hernani Rodrigues Vaz
module Cns
  # classe para acesso dados blockchain ETH
  class Apies
    # @return [String] apikey a juntar aos pedidos HTTP url:
    attr_reader :key
    # @return [String] base URL to use as a prefix for all requests
    attr_reader :url

    # @param [String] chv apikey a juntar aos pedidos HTTP url:
    # @param [String] www base URL to use as a prefix for all requests
    # @return [Apies] API etherscan base
    def initialize(chv: ENV['ETHERSCAN_API_KEY'], www: 'https://api.etherscan.io/')
      @key = chv
      @url = www
    end

    # @return [<Symbol>] adapter for the connection - default :net_http
    def adapter
      @adapter ||= Faraday.default_adapter
    end

    # @return [<Faraday::Connection>] connection object with an URL & adapter
    def conn
      @conn ||=
        Faraday.new(url: url) do |c|
          c.request(:url_encoded)
          c.adapter(adapter)
        end
    end

    # @example
    #  [
    #    { account: '0x...', balance: '4000000000000000000' },
    #    { account: '0x...', balance: '87000000000000000000' }
    #  ]
    # @param [String] ads lista enderecos carteiras ETH (max 20)
    # @return [Array<Hash>] devolve lista com dados & saldo de carteiras ETH
    def account(ads)
      raise(Erro, 'maximo de 20 enderecos') if ads.size > 20

      get(action: 'balancemulti', address: ads.join(','), tag: 'latest')[:result]
    end

    # @example
    #  [
    #    {
    #      blockNumber: '4984535',
    #      timeStamp: '1517094794',
    #      hash: '0x...',
    #      nonce: '10',
    #      blockHash: '0x...',
    #      transactionIndex: '17',
    #      from: '0x...',
    #      to: '0x...',
    #      value: '52627271000000000000',
    #      gas: '21000',
    #      gasPrice: '19000000000',
    #      isError: '0',
    #      txreceipt_status: '1',
    #      input: '0x',
    #      contractAddress: '',
    #      gasUsed: '21000',
    #      cumulativeGasUsed: '566293',
    #      confirmations: '5848660'
    #    },
    #    {}
    #  ]
    # @param [String] add endereco carteira ETH
    # @param [Hash] arg argumentos trabalho
    # @option arg [Integer] :start_block starting blockNo to retrieve results
    # @option arg [Integer] :end_block ending blockNo to retrieve results
    # @option arg [String] :sort asc -> ascending order, desc -> descending order
    # @option arg [Integer] :page to get paginated results
    # @option arg [Integer] :offset max records to return
    # @return [Array<Hash>] lista de transacoes
    def norml_tx(add, **arg)
      raise(Erro, 'endereco tem de ser definido') if add.nil? || add.empty?

      ledger(**arg.merge(action: 'txlist', address: add))
    end

    # @example registo duplicado
    #  [
    #    {
    #      blockNumber: '3967652',
    #      timeStamp: '1499081515',
    #      hash: '0x registo duplicado com todos os dados iguais',
    #      nonce: '3',
    #      blockHash: '0x00a49e999036dc13dc6c4244bb1d51d3146fe7f00bfb500a7624d82e299c7328',
    #      from: '0xd0a6e6c54dbc68db5db3a091b171a77407ff7ccf',
    #      contractAddress: '0x86fa049857e0209aa7d9e616f7eb3b3b78ecfdb0',
    #      to: '0x...',
    #      value: '0',
    #      tokenName: 'EOS',
    #      tokenSymbol: 'EOS',
    #      tokenDecimal: '18',
    #      transactionIndex: '83',
    #      gas: '173399',
    #      gasPrice: '21000000000',
    #      gasUsed: '173398',
    #      input: 'deprecated',
    #      cumulativeGasUsed: '7484878',
    #      confirmations: '3442641'
    #    },
    #    {}
    #  ]
    # @param add (see norml_tx)
    # @param [String] cdd token address (nil to get a list of all ERC20 transactions)
    # @param arg (see norml_tx)
    # @option arg (see norml_tx)
    # @return [Array<Hash>] lista de token transfer events
    def token_tx(add, cdd = nil, **arg)
      raise(Erro, 'contrato ou endereco tem de estar definido') if (cdd || add).nil? || (cdd || add).empty?

      # registos duplicados aparecem em token events (ver exemplo acima)
      # -quando ha erros na blockchain (acho)
      # -quando ha token events identicos no mesmo block (acho)
      ledger(**arg.merge(action: 'tokentx', address: add, contractaddress: cdd))
    end

    # @param [Integer] pag pagina das transacoes a devolver
    # @param [Array<Hash>] ary lista acumuladora das transacoes a devolver
    # @param arg (see norml_tx)
    # @option arg (see norml_tx)
    # @return [Array<Hash>] devolve lista de transacoes/token transfer events
    def ledger(pag = 0, ary = [], **arg)
      r = get(**arg.merge(page: pag + 1, offset: 10_000))[:result]
      ary += r
      r.count < 10_000 ? ary : ledger(pag + 1, ary, **arg)
    rescue StandardError
      ary
    end

    private

    # @example
    #  {
    #    status: '1',
    #    message: 'OK',
    #    result: []
    #  }
    # @return [Hash] resultado do HTTP request
    def get(**arg)
      JSON.parse(
        (conn.get('api') do |o|
           o.headers = { content_type: 'application/json', accept: 'application/json', user_agent: 'etherscan;ruby' }
           o.params = arg.merge(module: 'account', apikey: key).reject { |_, v| v.nil? }
         end).body,
        symbolize_names: true
      )
    rescue StandardError
      { result: [] }
    end
  end
end
