# frozen_string_literal: true

require('openssl')
require('base64')
require('curb')
require('json')

# @author Hernani Rodrigues Vaz
module Cns
  # classe para acesso dados centralized exchanges
  class Apice
    API = {de: 'https://api.bitcoin.de/v4', us: 'https://api.kraken.com/0/private'}.freeze

    def initialize
      @curl =
        Curl::Easy.new.tap do |c|
          c.timeout = 30
          c.connect_timeout = 10
          c.follow_location = true
          c.ssl_verify_peer = true
        end
      @deky = ENV.fetch('BITCOINDE_API_KEY', nil)
      @desc = ENV.fetch('BITCOINDE_API_SECRET', nil)
      @usky = ENV.fetch('KRAKEN_API_KEY', nil)
      @ussc = ENV.fetch('KRAKEN_API_SECRET', nil)
    end

    # Get account balances from Bitcoin.de
    # @return [Hash] saldos no bitcoinde
    def account_de
      uri = "#{API[:de]}/account"
      rcrl(@curl, uri, headers: hde(uri))
      pjsn(@curl).dig(:data, :balances) || {}
    rescue Curl::Err::CurlError
      {}
    end

    # Get trades from Bitcoin.de
    # @return [Array<Hash>] trades bitcoinde
    def trades_de
      pag_de_req("#{API[:de]}/trades", {state: 1}, :trades)
    rescue Curl::Err::CurlError
      []
    end

    # Get deposits from Bitcoin.de, uniformly formatted
    # @return [Array<Hash>] depositos uniformizados bitcoinde
    def deposits_de
      pag_de_req("#{API[:de]}/btc/deposits", {state: 2}, :deposits) { |i| i.map { |h| deposit_unif(h) } }
    rescue Curl::Err::CurlError
      []
    end

    # Get withdrawals from Bitcoin.de, uniformly formatted
    # @return [Array<Hash>] withdrawals uniformizadas bitcoinde
    def withdrawals_de
      pag_de_req("#{API[:de]}/btc/withdrawals", {state: 1}, :withdrawals) { |i| i.map { |h| withdrawal_unif(h) } }
    rescue Curl::Err::CurlError
      []
    end

    # Get account balances from Kraken
    # @return [Hash] saldos kraken
    def account_us
      uri = 'Balance'
      ops = {nonce: nnc}
      rcrl(@curl, "#{API[:us]}/#{uri}", method: 'POST', post_data: ops, headers: hus(uri, ops))
      pjsn(@curl).fetch(:result, {})
    rescue Curl::Err::CurlError
      {}
    end

    # Get trades from Kraken
    # @param [Integer] days optional number of days to fetch trades from (last N days)
    # @return [Hash] trades kraken
    def trades_us(days = nil)
      pag_us_req('TradesHistory', :trades, days ? {start: days} : {})
    rescue Curl::Err::CurlError
      []
    end

    # Get ledger from Kraken
    # @param [Integer] days optional number of days to fetch ledger from (last N days)
    # @return [Hash] ledger kraken
    def ledger_us(days = nil)
      pag_us_req('Ledgers', :ledger, days ? {start: days} : {})
    rescue Curl::Err::CurlError
      []
    end

    private

    # Uniformly format transacao kraken
    # @param [Symbol] key id da transacao
    # @param [Hash] trx transacao
    # @return [Hash] transacao uniformizada
    def us_unif(key, trx)
      t = trx[:time].to_i
      trx.merge(txid: key.to_s, srx: t, time: Time.at(t))
    rescue ArgumentError
      trx.merge(txid: key.to_s, srx: 0, time: Time.at(0))
    end

    # Generic paginated request handler for Kraken
    # @param [String] uri API endpoint URI
    # @param [Symbol] key Key to extract from the result
    # @param [Hash] prm Additional options for the request
    # @yield [Array<Hash>] Block to process each batch of results
    # @return [Array<Hash>] Combined results from all pages
    def pag_us_req(uri, key, prm = {})
      ary = []
      ofs = 0
      loop do
        # Rate limiting for page requests (2s in Kraken)
        sleep(@lpag - Time.now + 2) if @lpag && Time.now - @lpag < 2
        ops = prm.merge({nonce: nnc, ofs: ofs})
        rcrl(@curl, "#{API[:us]}/#{uri}", method: 'POST', post_data: ops, headers: hus(uri, ops))
        bth = pjsn(@curl).fetch(:result, {}).fetch(key, []).map { |k, v| us_unif(k, v) }
        break if bth.empty?

        ary.concat(bth)
        ofs += bth.size
        @lpag = Time.now
      end
      ary
    end

    # Generic paginated request handler for Bitcoin.de
    # @param [String] uri Base URL for the API request
    # @param [Hash] prm Additional parameters for the request
    # @param [Symbol] key Key to extract from the result
    # @yield [Array<Hash>] Optional block to process each batch of results
    # @return [Array<Hash>] Combined results from all pages
    def pag_de_req(uri, prm, key)
      ary = []
      pag = 1
      loop do
        url = "#{uri}?#{URI.encode_www_form(prm.merge(page: pag))}"
        rcrl(@curl, url, headers: hde(url))
        res = pjsn(@curl)
        bth = res.fetch(key, [])
        ary.concat(block_given? ? yield(bth) : bth)
        break if res[:page]&.fetch(:current, 0)&.>= res[:page]&.fetch(:last, 0)

        pag += 1
      end
      ary
    end

    # Configure Curl object for request
    # @param [Curl::Easy] curl Curl object to configure
    # @param [String] url URL for the request
    # @param [String] method HTTP method (GET or POST)
    # @param [Hash] post_data Data to send in POST requests
    # @param [Hash] headers HTTP headers for the request
    def rcrl(curl, url, method: 'GET', post_data: nil, headers: {})
      curl.reset
      curl.url = url
      curl.http(method)
      curl.headers = headers
      curl.post_body = URI.encode_www_form(post_data) if post_data
      curl.perform
    end

    # Safe JSON parsing with error handling
    # @param [Curl::Easy] res Curl response object
    # @return [Hash] Parsed JSON or empty hash on error
    def pjsn(res)
      return {} if res.nil? || res.body_str.to_s.empty?

      JSON.parse(res.body_str, symbolize_names: true) || {}
    rescue JSON::ParserError
      {}
    end

    # Generate a continually-increasing unsigned integer nonce from the current Unix Time
    # @return [Integer] Nonce value
    def nnc
      Process.clock_gettime(Process::CLOCK_REALTIME, :nanosecond).to_i
    end

    # Uniformly format a deposit from Bitcoin.de
    # @param [Hash] has Deposit data from Bitcoin.de
    # @return [Hash] deposito uniformizado bitcoinde
    def deposit_unif(has)
      {add: has[:address], time: Time.parse(has[:created_at]), qtd: has[:amount].to_d, nxid: has[:deposit_id].to_i}.merge(tp: 'deposit', moe: 'BTC', fee: 0.to_d)
    end

    # Uniformly format a withdrawal from Bitcoin.de
    # @param [Hash] has Withdrawal data from Bitcoin.de
    # @return [Hash] withdrawal uniformizada bitcoinde
    def withdrawal_unif(has)
      {
        add: has[:address],
        time: Time.parse(has[:transferred_at]),
        qtd: -1 * has[:amount].to_d,
        fee: has[:network_fee].to_d,
        nxid: has[:withdrawal_id].to_i,
        tp: 'withdrawal',
        moe: 'BTC'
      }
    end

    # Generate headers for Bitcoin.de HTTP requests
    # @param [String] qde Query string to include in the HTTP request
    # @param [Integer] non Nonce value (default: generated from nnc)
    # @return [Hash] Headers required for Bitcoin.de HTTP requests
    def hde(qde, non = nnc)
      md5 = ['GET', qde, @deky, non, Digest::MD5.hexdigest('')].join('#')
      mac = OpenSSL::HMAC.hexdigest('sha256', @desc, md5)
      {'X-API-KEY' => @deky, 'X-API-NONCE' => non.to_s, 'X-API-SIGNATURE' => mac}
    rescue OpenSSL::HMACError => e
      raise("HMAC bitcoinde generation failed: #{e.message}")
    end

    # Generate headers for Kraken HTTP requests
    # @param [String] qus query a incluir no pedido HTTP
    # @param [Hash] ops opcoes trabalho
    # @option ops [Hash] :nonce continually-increasing unsigned integer
    # @return [Hash] Headers required for Kraken HTTP requests
    def hus(qus, ops)
      sha = ['/0/private/', qus, Digest::SHA256.digest("#{ops[:nonce]}#{URI.encode_www_form(ops)}")].join
      mac = OpenSSL::HMAC.digest('sha512', Base64.decode64(@ussc), sha)
      {'api-key' => @usky, 'api-sign' => Base64.strict_encode64(mac)}
    rescue OpenSSL::HMACError => e
      raise("HMAC kraken generation failed: #{e.message}")
    end
  end
end
