# frozen_string_literal: true

require('openssl')
require('base64')
require('curb')
require('json')

# @author Hernani Rodrigues Vaz
module Cns
  # classe para processar dados no paymium
  class Apifr
    # @return [String] API key
    attr_reader :aky
    # @return [String] API secret
    attr_reader :asc
    # @return [String] API url base
    attr_reader :urb

    # @param [String] pky API key
    # @param [String] psc API secret
    # @param [Hash] ops parametrizacao base da API
    # @return [Apius] API paymium base
    def initialize(
      pky: ENV['PAYMIUM_API_KEY'],
      psc: ENV['PAYMIUM_API_SECRET'],
      ops: { www: 'https://paymium.com', ver: 1 }
    )
      @aky = pky
      @asc = psc
      @urb = "#{ops[:www]}/api/v#{ops[:ver]}"
    end

    # @example
    #  {
    #    name: '...',
    #    email: '...',
    #    locale: 'en',
    #    channel_id: '...',
    #    meta_state: 'approved',
    #    balance_eur: '0.0',
    #    locked_eur: '0.0',
    #    balance_btc: '0.0',
    #    locked_btc: '0.0',
    #    balance_lbtc: '0.0',
    #    locked_lbtc: '0.0'
    #  }
    # @return [Hash] saldos no paymium
    def account
      api_get('user')
    end

    # @example
    #  [
    #    {
    #      uuid: '50551e61-4e74-4ae7-85fd-9c2040542818',
    #      currency_amount: nil,
    #      state: 'executed',
    #      btc_fee: '0.0',
    #      currency_fee: '0.0',
    #      created_at: '2014-03-04T09:00Z',
    #      updated_at: '2014-03-04T09:00Z',
    #      currency: 'EUR',
    #      comment: '5723',
    #      amount: '100.0',
    #      type: 'WireDeposit',
    #      account_operations: [{
    #        uuid: 'b5058a68-cf99-4438-86d3-e773eba418ec',
    #        name: 'wire_deposit',
    #        amount: '100.0',
    #        currency: 'EUR',
    #        created_at: '2014-03-04T09:00Z',
    #        created_at_int: 1_393_923_644,
    #        is_trading_account: false
    #      }, {}]
    #    }, {}
    #  ]
    # @return [Hash] ledger no paymium
    def ledger(pag = 0, ary = [])
      r = api_get('user/orders', offset: pag)
      r.empty? ? ary : ledger(pag + r.size, ary + r)
    rescue StandardError
      ary
    end

    private

    # HTTP GET request for public paymium API queries.
    def api_get(uri, **ops)
      resposta(Curl.get("#{urb}/#{uri}", ops) { |r| r.headers = hdrs(url(uri, ops), nonce, {}) })
    end

    # HTTP POST request for private paymium API queries involving user credentials.
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
        'Api-Key': aky,
        'Api-Nonce': non,
        'Api-Signature': auth(qry, non, URI.encode_www_form(ops))
      }
    end

    # @return [String] assinarura codificada dos pedidos HTTP
    def auth(qry, non, par)
      raise(ArgumentError, 'API Key is not set') unless aky
      raise(ArgumentError, 'API Secret is not set') unless asc

      OpenSSL::HMAC.hexdigest('sha256', asc, [non, "#{urb}/#{qry}", par].join)
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
      "Erro da API paymium #{e.inspect}"
    end
  end
end
