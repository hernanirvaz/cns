# frozen_string_literal: true

require('openssl')
require('base64')
require('curb')
require('json')

# @author Hernani Rodrigues Vaz
module Cns
  DC = %w[LTC NMC PPC DOGE XRP Linden USD CAD GBP ZEC BCH EURN NOKU FDZ GUSD SEED USDC].freeze

  # classe para processar dados no therock
  class Apimt
    # @return [String] API key
    attr_reader :aky
    # @return [String] API secret
    attr_reader :asc
    # @return [String] API url base
    attr_reader :urb

    # @param [String] pky API key
    # @param [String] psc API secret
    # @param [Hash] ops parametrizacao base da API
    # @return [Apius] API therock base
    def initialize(
      pky: ENV['THEROCK_API_KEY'],
      psc: ENV['THEROCK_API_SECRET'],
      ops: { www: 'https://api.therocktrading.com', ver: 1 }
    )
      @aky = pky
      @asc = psc
      @urb = "#{ops[:www]}/v#{ops[:ver]}"
    end

    # @example
    #  {
    #    balances: [
    #      { currency: 'BTC', balance: 0.0, trading_balance: 0.0 },
    #      { currency: 'ETH', balance: 0.0, trading_balance: 0.0 },
    #      { currency: 'EUR', balance: 0.0, trading_balance: 0.0 },
    #      { currency: 'DAI', balance: 0.0, trading_balance: 0.0 },
    #    ]
    #  }
    # @return [Hash] saldos no therock
    def account
      api_get('balances')[:balances].delete_if { |e| DC.include?(e[:currency]) }
                                    .sort { |a, b| a[:currency] <=> b[:currency] }
    end

    # @example
    #  {
    #    transactions: [
    #      {
    #        id: 305_445,
    #        date: '2014-03-06T10:59:13.000Z',
    #        type: 'withdraw',
    #        price: 97.47,
    #        currency: 'EUR',
    #        fund_id: nil,
    #        order_id: nil,
    #        trade_id: nil,
    #        note: 'BOV withdraw',
    #        transfer_detail: nil
    #      },
    #      {}
    #    ],
    #    meta: {
    #      total_count: nil,
    #      first: { page: 1, href: 'https://api.therocktrading.com/v1/transactions?page=1' },
    #      previous: nil,
    #      current: { page: 1, href: 'https://api.therocktrading.com/v1/transactions?page=1' },
    #      next: { page: 2, href: 'https://api.therocktrading.com/v1/transactions?page=2' },
    #      last: nil
    #    }
    #  }
    # @return [Hash] ledger no therock
    def ledger(pag = 1, ary = [])
      r = api_get('transactions', page: pag)[:transactions]
      r.empty? ? ary : ledger(pag + r.size, ary + r)
    rescue StandardError
      ary
    end

    private

    # HTTP GET request for public therock API queries.
    def api_get(uri, **ops)
      resposta(Curl.get("#{urb}/#{uri}", ops) { |r| r.headers = hdrs(url(uri, ops), nonce, {}) })
    end

    # HTTP POST request for private therock API queries involving user credentials.
    def api_post(uri, **ops)
      resposta(Curl.post("#{urb}/#{uri}", ops) { |r| r.headers = hdrs(uri, nonce, ops) })
    end

    # @return [String] URL do pedido formatado com todos os parametros
    def url(uri, ops)
      ops.empty? ? uri : "#{uri}?#{URI.encode_www_form(ops)}"
    end

    # @return [Hash] headers necessarios para pedido HTTP
    def hdrs(qry, non, ops)
      {
        content_type: 'application/json',
        'X-TRT-KEY': aky,
        'X-TRT-NONCE': non,
        'X-TRT-SIGN': auth(qry, non, URI.encode_www_form(ops))
      }
    end

    # @return [String] assinarura codificada dos pedidos HTTP
    def auth(qry, non, par)
      raise(ArgumentError, 'API Key is not set') unless aky
      raise(ArgumentError, 'API Secret is not set') unless asc

      OpenSSL::HMAC.hexdigest('sha512', asc, [non, "#{urb}/#{qry}", par].join)
    end

    # @return [Integer] continually-increasing unsigned integer nonce from the current Unix Time
    def nonce
      Integer(Float(Time.now) * 1e6)
    end

    # @return [Hash] resposta do pedido HTTP
    def resposta(http)
      http.response_code == 200 ? JSON.parse(http.body, symbolize_names: true) : http.status
    rescue JSON::ParserError,
           EOFError,
           Errno::ECONNRESET,
           Errno::EINVAL,
           Net::HTTPBadResponse,
           Net::HTTPHeaderSyntaxError,
           Net::ProtocolError,
           Timeout::Error => e
      "Erro da API therock #{e.inspect}"
    end
  end
end
