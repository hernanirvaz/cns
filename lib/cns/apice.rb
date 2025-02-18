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
    # @return [Hash] saldos no bitcoinde
    def account_de
      uri = "#{API[:de]}/account"
      parse_json(Curl.get(uri) { |obj| obj.headers = hde(uri) }).dig(:data, :balances) || {}
    rescue Curl::Err::CurlError
      {}
    end

    # @return [Array<Hash>] trades bitcoinde
    def trades_de
      pag = 1
      ary = []
      loop do
        url = "#{API[:de]}/trades?#{URI.encode_www_form(state: 1, page: pag)}"
        data = parse_json(Curl.get(url) { |obj| obj.headers = hde(url) })
        ary += data.fetch(:trades, [])
        break if data[:page][:current] >= data[:page][:last]

        pag += 1
      end
      ary
    rescue Curl::Err::CurlError
      ary
    end

    # @return [Array<Hash>] depositos uniformizados bitcoinde
    def deposits_de
      pag = 1
      ary = []
      loop do
        url = "#{API[:de]}/btc/deposits?#{URI.encode_www_form(state: 2, page: pag)}"
        data = parse_json(Curl.get(url) { |obj| obj.headers = hde(url) })
        ary += data.fetch(:deposits, []).map { |has| deposit_unif(has) }
        break if data[:page][:current] >= data[:page][:last]

        pag += 1
      end
      ary
    rescue Curl::Err::CurlError
      ary
    end

    # @return [Hash] deposito uniformizado bitcoinde
    def deposit_unif(has)
      { add: has[:address], time: Time.parse(has[:created_at]), qt: has[:amount], txid: Integer(has[:deposit_id]) }.merge(tp: 'deposit', moe: 'btc', fee: '0')
    end

    # @return [Array<Hash>] withdrawals uniformizadas bitcoinde
    def withdrawals_de
      ary = []
      pag = 1
      loop do
        url = "#{API[:de]}/btc/withdrawals?#{URI.encode_www_form(state: 1, page: pag)}"
        data = parse_json(Curl.get(url) { |obj| obj.headers = hde(url) })
        ary += data.fetch(:withdrawals, []).map { |has| withdrawal_unif(has) }
        break if data[:page][:current] >= data[:page][:last]

        pag += 1
      end
      ary
    rescue Curl::Err::CurlError
      ary
    end

    # @return [Hash] withdrawal uniformizada bitcoinde
    def withdrawal_unif(has)
      {
        add: has[:address],
        time: Time.parse(has[:transferred_at]),
        qt: has[:amount],
        fee: has[:network_fee],
        txid: Integer(has[:withdrawal_id])
      }.merge(tp: 'withdrawal', moe: 'btc')
    end

    # @return [Hash] saldos kraken
    def account_us
      uri = 'Balance'
      ops = { nonce: nnc }
      parse_json(Curl.post("#{API[:us]}/#{uri}", ops) { |hed| hed.headers = hus(uri, ops) }).fetch(:result, {})
    rescue Curl::Err::CurlError
      {}
    end

    # @return [Hash] trades kraken
    def trades_us
      uri = 'TradesHistory'
      has = {}
      ofs = 0
      loop do
        sleep(1)
        ops = { nonce: nnc, ofs: ofs }
        result = parse_json(Curl.post("#{API[:us]}/#{uri}", ops) { |hed| hed.headers = hus(uri, ops) }).fetch(:result, {})
        break if result.fetch(:trades, []).empty?

        has.merge!(result[:trades])
        ofs += result[:trades].size
      end
      has
    rescue Curl::Err::CurlError
      has
    end

    # @return [Hash] ledger kraken
    def ledger_us
      uri = 'Ledgers'
      has = {}
      ofs = 0
      loop do
        sleep(2)
        ops = { nonce: nnc, ofs: ofs }
        result = parse_json(Curl.post("#{API[:us]}/#{uri}", ops) { |hed| hed.headers = hus(uri, ops) }).fetch(:result, {})
        break if result.fetch(:ledger, []).empty?

        has.merge!(result[:ledger])
        ofs += result[:ledger].size
      end
      has
    rescue Curl::Err::CurlError
      has
    end

    private

    # Safe JSON parsing with error handling
    def parse_json(res)
      JSON.parse(res.body, symbolize_names: true) || {}
    rescue JSON::ParserError
      {}
    end

    # @return [Integer] continually-increasing unsigned integer nonce from the current Unix Time
    def nnc
      Integer(Float(Time.now) * 1e6)
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
