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
      @escn = bccn('https://api.etherscan.io')
      @gmcn = bccn('https://eos.greymass.com')
      @esky = ENV.fetch('ETHERSCAN_API_KEY', nil)
      @blks = {} # Cache to store block numbers so we don't ask API repeatedly
    end

    # Get account balances for multiple ETH addresses
    # @param [Array<String>] ads List of ETH addresses (max 20)
    # @return [Array<Hash>] List of addresses with balances
    def account_es(ads)
      return [] if ads.nil? || ads.empty?

      # Batch addresses into groups of 20 (Etherscan limit) and fetch balances
      ads.each_slice(20).flat_map do |b|
        res = es_req('balancemulti', b.join(','), 1, tag: 'latest')
        res[:status] == '1' ? Array(res[:result]) : []
      end
    rescue StandardError
      []
    end

    # Get normal transactions for ETH address
    # @param [String] add endereco ETH
    # @param [Integer] days (Optional) Fetch only last N days
    # @return [Array<Hash>] lista transacoes normais etherscan
    def norml_es(add, days: nil)
      prm = days ? {startblock: start_block(days)} : {}
      pag_es_req('txlist', add, prm)
    end

    # Get internal transactions for ETH address
    # @param (see norml_es)
    # @return [Array<Hash>] lista transacoes internas etherscan
    def inter_es(add, days: nil)
      prm = days ? {startblock: start_block(days)} : {}
      pag_es_req('txlistinternal', add, prm)
    end

    # Get mined blocks for ETH address
    # @param (see norml_es)
    # @return [Array<Hash>] lista blocos etherscan
    def block_es(add)
      pag_es_req('getminedblocks', add, blocktype: 'blocks')
    end

    # Get withdrawals for ETH address
    # @param (see norml_es)
    # @return [Array<Hash>] lista blocos etherscan
    def withw_es(add, days: nil)
      prm = days ? {startblock: start_block(days)} : {}
      pag_es_req('txsBeaconWithdrawal', add, prm)
    end

    # Get token transfers for ETH address
    # @param (see norml_es)
    # @return [Array<Hash>] lista token transfer events etherscan
    def token_es(add, days: nil)
      prm = days ? {startblock: start_block(days)} : {}
      pag_es_req('tokentx', add, prm)
    end

    # Get EOS account information
    # @param [String] add EOS account name
    # @return [Hash] Account details with resources
    def account_gm(add)
      res = gm_req('/v1/chain/get_account', account_name: add)
      res[:core_liquid_balance]&.to_d&.positive? ? res : gm_erro
    end

    # Get complete transaction history for EOS account
    # @param (see account_gm)
    # @return [Array<Hash>] lista completa transacoes greymass
    def ledger_gm(add)
      trx = []
      pos = 0
      loop do
        res = gm_req('/v1/history/get_actions', account_name: add, pos: pos, offset: GMPS)
        bth = Array(res[:actions])
        trx.concat(bth)
        break if bth.size < GMPS

        pos += GMPS
      end
      trx
    rescue StandardError
      trx
    end

    private

    # Calculate (and cache) the block number for N days ago
    def start_block(days)
      return 0 if days.nil?
      return @blks[days] if @blks.key?(days)

      res = block_req(Integer(Time.now - (days * 86_400)))
      if res[:status] == '1'
        blk = Integer(res[:result], 10)
        @blks[days] = blk
        blk
      else
        0
      end
    rescue StandardError
      0
    end

    # New dedicated method for Block API calls
    def block_req(timestamp)
      prm = {chainid: 1, module: 'block', action: 'getblocknobytime', timestamp: timestamp, closest: 'after', apikey: @esky}
      pjsn(@escn.get('/v2/api', prm))
    rescue Faraday::Error
      {status: '0'}
    end

    # Make a request to the Etherscan API
    # @param [String] act API action name
    # @param [String] add Blockchain address
    # @param [Integer] pag Page number for pagination
    # @param [Hash] prm Additional request parameters
    # @return [Hash] Parsed API response
    def es_req(act, add, pag = 1, prm = {})
      pjsn(@escn.get('/v2/api', prm.merge(chainid: 1, module: 'account', action: act, address: add, page: pag, apikey: @esky)))
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

        bth = Array(res[:result])
        trx.concat(bth)
        break if bth.size < ESPS

        pag += 1
      end
      trx
    rescue StandardError
      trx
    end

    # Make a request to the Greymass API
    # @param [String] url API endpoint
    # @param [Hash] pyl Request payload
    # @return [Hash] Parsed API response
    def gm_req(url, pyl)
      pjsn(@gmcn.post(url) { |r| r.body = pyl })
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
    def pjsn(res)
      return {} if res.nil? || res.body.to_s.empty?

      JSON.parse(res.body, symbolize_names: true) || {}
    rescue JSON::ParserError
      {}
    end

    # Create a Faraday connection with JSON configuration and retry logic
    # @param [String] url Base URL for the API
    # @return [Faraday::Connection] Configured Faraday connection
    def bccn(url)
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
