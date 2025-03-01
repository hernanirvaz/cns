# frozen_string_literal: true

require('openssl')
require('base64')
require('curb')
require('json')

# @author Hernani Rodrigues Vaz
module Cns
  API = { de: 'https://api.bitcoin.de/v4', us: 'https://api.kraken.com/0/private' }.freeze

  # classe para acesso dados centralized exchanges
  class Apice
    def initialize
      @curl = Curl::Easy.new
      @curl.timeout = 30
      @curl.connect_timeout = 10
      @curl.follow_location = true
      @curl.ssl_verify_peer = true
    end

    # @return [Hash] saldos no bitcoinde
    def account_de
      uri = "#{API[:de]}/account"
      run_curl(@curl, uri, headers: hde(uri))
      parse_json(@curl).dig(:data, :balances) || {}
    rescue Curl::Err::CurlError
      {}
    end

    # @return [Array<Hash>] trades bitcoinde
    def trades_de
      pag_de_req("#{API[:de]}/trades", { state: 1 }, :trades)
    rescue Curl::Err::CurlError
      []
    end

    # @return [Array<Hash>] depositos uniformizados bitcoinde
    def deposits_de
      pag_de_req("#{API[:de]}/btc/deposits", { state: 2 }, :deposits) { |i| i.map { |h| deposit_unif(h) } }
    rescue Curl::Err::CurlError
      []
    end

    # @return [Array<Hash>] withdrawals uniformizadas bitcoinde
    def withdrawals_de
      pag_de_req("#{API[:de]}/btc/withdrawals", { state: 1 }, :withdrawals) { |i| i.map { |h| withdrawal_unif(h) } }
    rescue Curl::Err::CurlError
      []
    end

    # @return [Hash] saldos kraken
    def account_us
      uri = 'Balance'
      ops = { nonce: nnc }
      run_curl(@curl, "#{API[:us]}/#{uri}", method: :post, post_data: ops, headers: hus(uri, ops))
      parse_json(@curl).fetch(:result, {})
    rescue Curl::Err::CurlError
      {}
    end

    # @return [Hash] trades kraken
    def trades_us
      pag_us_req('TradesHistory', :trades)
    rescue Curl::Err::CurlError
      {}
    end

    # @return [Hash] ledger kraken
    def ledger_us
      pag_us_req('Ledgers', :ledger)
    rescue Curl::Err::CurlError
      {}
    end

    private

    # Generic paginated request handler for Kraken
    def pag_us_req(uri, key)
      has = {}
      ofs = 0
      loop do
        sleep(ofs.zero? ? 0 : 2)
        ops = { nonce: nnc, ofs: ofs }
        run_curl(@curl, "#{API[:us]}/#{uri}", method: :post, post_data: ops, headers: hus(uri, ops))
        batch = parse_json(@curl).fetch(:result, {}).fetch(key, [])
        break if batch.empty?

        has.merge!(batch)
        ofs += batch.size
      end
      has
    end

    # Generic paginated request handler for Bitcoin.de
    def pag_de_req(base_url, params, key)
      ary = []
      pag = 1
      loop do
        url = "#{base_url}?#{URI.encode_www_form(params.merge(page: pag))}"
        run_curl(@curl, url, headers: hde(url))
        result = parse_json(@curl)
        batch = result.fetch(key, [])
        ary.concat(block_given? ? yield(batch) : batch)
        break if result[:page]&.[](:current)&.>= result[:page]&.[](:last)

        pag += 1
      end
      ary
    end

    # Configure Curl object for request
    def run_curl(curl, url, method: :get, post_data: nil, headers: {})
      curl.reset
      curl.url = url
      curl.http(method == :post ? 'POST' : 'GET')
      curl.headers = headers
      curl.post_body = URI.encode_www_form(post_data) if post_data
      curl.perform
    end

    # Safe JSON parsing with error handling
    def parse_json(res)
      JSON.parse(res.body_str, symbolize_names: true)
    rescue JSON::ParserError
      {}
    end

    # @return [Integer] continually-increasing unsigned integer nonce from the current Unix Time
    def nnc
      Integer(Float(Time.now) * 1e6)
    end

    # @return [Hash] deposito uniformizado bitcoinde
    def deposit_unif(has)
      { add: has[:address], time: Time.parse(has[:created_at]), qt: has[:amount], txid: Integer(has[:deposit_id]) }.merge(tp: 'deposit', moe: 'btc', fee: '0')
    end

    # @return [Hash] withdrawal uniformizada bitcoinde
    def withdrawal_unif(has)
      {
        add: has[:address],
        time: Time.parse(has[:transferred_at]),
        qt: has[:amount],
        fee: has[:network_fee],
        txid: Integer(has[:withdrawal_id]),
        tp: 'withdrawal',
        moe: 'btc'
      }
    end

    # @param [String] qde query a incluir no pedido HTTP
    # @param [Integer] non continually-increasing unsigned integer
    # @return [Hash] headers necessarios para pedido HTTP da exchange bitcoinde
    def hde(qde, non = nnc)
      key = ENV.fetch('BITCOINDE_API_KEY', nil)
      md5 = ['GET', qde, key, non, Digest::MD5.hexdigest('')].join('#')
      mac = OpenSSL::HMAC.hexdigest('sha256', ENV.fetch('BITCOINDE_API_SECRET', nil), md5)
      { 'X-API-KEY': key, 'X-API-NONCE': non, 'X-API-SIGNATURE': mac }
    end

    # @param [String] qus query a incluir no pedido HTTP
    # @param [Hash] ops opcoes trabalho
    # @option ops [Hash] :nonce continually-increasing unsigned integer
    # @return [Hash] headers necessarios para pedido HTTP da exchange kraken
    def hus(qus, ops)
      key = ENV.fetch('KRAKEN_API_KEY', nil)
      sha = ['/0/private/', qus, Digest::SHA256.digest("#{ops[:nonce]}#{URI.encode_www_form(ops)}")].join
      mac = OpenSSL::HMAC.digest('sha512', Base64.decode64(ENV.fetch('KRAKEN_API_SECRET', nil)), sha)
      { 'api-key': key, 'api-sign': Base64.strict_encode64(mac) }
    end
  end
end
