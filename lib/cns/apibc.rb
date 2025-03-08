# frozen_string_literal: true

require('faraday')
require 'faraday/retry'
require('json')

# @author Hernani Rodrigues Vaz
module Cns
  # classe para acesso dados blockchains
  class Apibc
    ESPS = 10_000 # Etherscan maximum records per page
    GMPS = 100    # Greymass maximum actions per request

    def initialize
      @escn = connection('https://api.etherscan.io')
      @gmcn = connection('https://eos.greymass.com')
      @esky = ENV.fetch('ETHERSCAN_API_KEY', nil)
    end

    # Get account balances for multiple ETH addresses
    # @param [Array<String>] addresses List of ETH addresses (max 20)
    # @return [Array<Hash>] List of addresses with balances
    def account_es(addresses)
      return [] if addresses.nil? || addresses.empty?

      # Batch addresses into groups of 20 (Etherscan limit) and fetch balances
      addresses.each_slice(20).flat_map do |b|
        res = es_req('balancemulti', b.join(','), 1, tag: 'latest')
        res[:status] == '1' ? res[:result] || [] : []
      end
    end

    # Get normal transactions for ETH address
    # @param [String] address endereco ETH
    # @return [Array<Hash>] lista transacoes normais etherscan
    def norml_es(address)
      pag_es_req('txlist', address)
    end

    # Get internal transactions for ETH address
    # @param (see norml_es)
    # @return [Array<Hash>] lista transacoes internas etherscan
    def inter_es(address)
      pag_es_req('txlistinternal', address)
    end

    # Get mined blocks for ETH address
    # @param (see norml_es)
    # @return [Array<Hash>] lista blocos etherscan
    def block_es(address)
      pag_es_req('getminedblocks', address, blocktype: 'blocks')
    end

    # Get withdrawals for ETH address
    # @param (see norml_es)
    # @return [Array<Hash>] lista blocos etherscan
    def withw_es(address)
      pag_es_req('txsBeaconWithdrawal', address)
    end

    # Get token transfers for ETH address
    # @param (see norml_es)
    # @return [Array<Hash>] lista token transfer events etherscan
    def token_es(address)
      pag_es_req('tokentx', address)
    end

    # Get EOS account information
    # @param [String] address EOS account name
    # @return [Hash] Account details with resources
    def account_gm(address)
      res = gm_req('/v1/chain/get_account', account_name: address)
      res[:core_liquid_balance]&.to_d&.positive? ? res : gm_erro
    end

    # Get complete transaction history for EOS account
    # @param (see account_gm)
    # @return [Array<Hash>] lista completa transacoes greymass
    def ledger_gm(address)
      trx = []
      pos = 0
      loop do
        res = gm_req('/v1/history/get_actions', account_name: address, pos: pos, offset: GMPS)
        bth = res[:actions] || []
        trx.concat(bth)
        break if bth.size < GMPS

        pos += GMPS
      end
      trx
    end

    private

    # Make a request to the Etherscan API
    # @param [String] act API action name
    # @param [String] add Blockchain address
    # @param [Integer] pag Page number for pagination
    # @param [Hash] prm Additional request parameters
    # @return [Hash] Parsed API response
    def es_req(act, add, pag = 1, prm = {})
      parse_json(@escn.get('/api', prm.merge(module: 'account', action: act, address: add, page: pag, apikey: @esky)))
    rescue Faraday::Error
      {status: '0'}
    end

    # Fetch paginated results from Etherscan
    # @param act [String] API action name
    # @param add [String] Blockchain address
    # @param prm [Hash] Additional request parameters
    # @return [Array<Hash>] Combined results from all pages
    def pag_es_req(act, add, prm = {})
      prm = prm.merge(offset: ESPS)
      trx = []
      pag = 1
      loop do
        res = es_req(act, add, pag, prm)
        break unless res[:status] == '1'

        bth = res[:result] || []
        trx.concat(bth)
        break if bth.size < ESPS

        pag += 1
      end
      trx
    end

    # Make a request to the Greymass API
    # @param [String] url API endpoint
    # @param [Hash] pyl Request payload
    # @return [Hash] Parsed API response
    def gm_req(url, pyl)
      parse_json(@gmcn.post(url) { |r| r.body = pyl })
    rescue Faraday::Error
      gm_erro
    end

    # Default error response for Greymass API
    # @return [Hash] Error response with zeroed values
    def gm_erro
      {core_liquid_balance: 0, total_resources: {net_weight: 0, cpu_weight: 0}}
    end

    # Safely parse JSON response
    # @param [Faraday::Response] res API response
    # @return [Hash] Parsed JSON or empty hash on error
    def parse_json(res)
      return {} if res.nil? || res.body.to_s.empty?

      JSON.parse(res.body, symbolize_names: true) || {}
    rescue JSON::ParserError
      {}
    end

    # Create a Faraday connection with JSON configuration and retry logic
    # @param [String] url Base URL for the API
    # @return [Faraday::Connection] Configured Faraday connection
    def connection(url)
      Faraday.new(url) do |c|
        c.request(:json)
        c.headers = {accept: 'application/json', user_agent: 'blockchain-api-client'}
        c.options.timeout = 30
        c.options.open_timeout = 10
        c.use(Faraday::Retry::Middleware, max: 3, interval: 1, backoff_factor: 2)
        c.adapter(Faraday.default_adapter)
      end
    end
  end
end
