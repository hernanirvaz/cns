# frozen_string_literal: true

require('openssl')
require('base64')
require('curb')
require('json')

# @author Hernani Rodrigues Vaz
module Cns
  # classe para processar dados no bitcoinde
  class Apide
    # @return [String] API key
    attr_reader :aky
    # @return [String] API secret
    attr_reader :asc
    # @return [String] API url base
    attr_reader :urb

    # @param [String] pky API key
    # @param [String] psc API secret
    # @param [Hash] ops parametrizacao base da API
    # @return [Apide] API bitcoinde base
    def initialize(
      pky: ENV['BITCOINDE_API_KEY'],
      psc: ENV['BITCOINDE_API_SECRET'],
      ops: { www: 'https://api.bitcoin.de', ver: 4 }
    )
      @aky = pky
      @asc = psc
      @urb = "#{ops[:www]}/v#{ops[:ver]}"
    end

    # @example
    #  {
    #    data: {
    #      balances: {
    #        btc: { total_amount: '0.00000000000000000000', available_amount: '0', reserved_amount: '0' },
    #        bch: { total_amount: '0.00000000000000000000', available_amount: '0', reserved_amount: '0' },
    #        btg: { total_amount: '0.00000000000000000000', available_amount: '0', reserved_amount: '0' },
    #        eth: { total_amount: '0.00000000000000000000', available_amount: '0', reserved_amount: '0' },
    #        bsv: { total_amount: '0.00000000000000000000', available_amount: '0', reserved_amount: '0' },
    #        ltc: { total_amount: '0.00000000000000000000', available_amount: '0', reserved_amount: '0' }
    #      },
    #      encrypted_information: { uid: '0y...', bic_short: '0y...', bic_full: '0y...' }
    #    },
    #    errors: [],
    #    credits: 23
    #  }
    # @return [Hash] saldos no bitcoinde
    def account
      api_get('account')[:data][:balances]
    end

    # @example
    #  {
    #    trades: [{
    #      trade_id: 'XUWWD3',
    #      trading_pair: 'btceur',
    #      is_external_wallet_trade: false,
    #      type: 'sell',
    #      amount_currency_to_trade: '0.1',
    #      price: 430,
    #      volume_currency_to_pay: 43,
    #      volume_currency_to_pay_after_fee: 42.79,
    #      amount_currency_to_trade_after_fee: 0.099,
    #      fee_currency_to_pay: 0.22,
    #      fee_currency_to_trade: '0.00100000',
    #      created_at: '2014-03-22T08:14:48+01:00',
    #      successfully_finished_at: '2014-03-25T14:03:22+01:00',
    #      state: 1,
    #      is_trade_marked_as_paid: true,
    #      trade_marked_as_paid_at: '2014-03-22T08:20:01+01:00',
    #      payment_method: 1,
    #      my_rating_for_trading_partner: 'positive',
    #      trading_partner_information: {
    #        username: 'emax2000',
    #        is_kyc_full: false,
    #        trust_level: 'bronze',
    #        amount_trades: 4,
    #        rating: 100,
    #        bank_name: 'CASSA DI RISPARMIO DI CIVITAVECCHIA SPA',
    #        bic: 'CRFIIT2CXXX',
    #        seat_of_bank: 'IT'
    #      }
    #    }, {}],
    #    page: { current: 1, last: 2 },
    #    errors: [],
    #    credits: 22
    #  }
    # @param [Integer] pag pagina dos dados a obter
    # @param [Array] ary lista acumuladora dos dados a obter
    # @return [Array<Hash>] lista trades no bitcoinde
    def trades(pag = 0, ary = [])
      r = api_get('trades', state: 1, page: pag + 1)
      ary += r[:trades]
      r[:page][:current] < r[:page][:last] ? trades(pag + 1, ary) : ary
    rescue StandardError
      ary
    end

    # @example
    #  {
    #    deposits: [{}, {}],
    #    page: { current: 1, last: 1 },
    #    errors: [],
    #    credits: 23
    #  }
    # @param (see trades)
    # @return [Array<Hash>] lista depositos no bitcoinde
    def deposits(pag = 0, ary = [])
      r = api_get('btc/deposits', state: 2, page: pag + 1)
      ary += r[:deposits].map { |h| deposit_hash(h) }
      r[:page][:current] < r[:page][:last] ? deposits(pag + 1, ary) : ary
    rescue StandardError
      ary
    end

    # @example
    #  {
    #    withdrawals: [{}, {}],
    #    page: { current: 1, last: 2 },
    #    errors: [],
    #    credits: 23
    #  }
    # @param (see trades)
    # @return [Array<Hash>] lista withdrawals no bitcoinde
    def withdrawals(pag = 0, ary = [])
      r = api_get('btc/withdrawals', state: 1, page: pag + 1)
      ary += r[:withdrawals].map { |h| withdrawal_hash(h) }
      r[:page][:current] < r[:page][:last] ? withdrawals(pag + 1, ary) : ary
    rescue StandardError
      ary
    end

    # @example
    #  {
    #    withdrawal_id: '136605',
    #    address: '1K9YMDDrmMV25EoYNqi7KUEK57Kn3TCNUJ',
    #    amount: '0.120087',
    #    network_fee: '0',
    #    comment: '',
    #    created_at: '2014-02-05T13:01:09+01:00',
    #    txid: '6264fe528116fcb87c812a306ca8409eecfec8fa941546c86f98984b882c8042',
    #    transferred_at: '2014-02-05T13:05:17+01:00',
    #    state: 1
    #  }
    # @param [Hash] hwi dados duma withdrawal
    # @return [Hash] withdrawal unifirmizada
    def withdrawal_hash(hwi)
      {
        id: hwi[:address],
        tp: 'out',
        qtxt: 'btc',
        fee: hwi[:network_fee],
        time: Time.parse(hwi[:transferred_at]),
        qt: hwi[:amount],
        lgid: Integer(hwi[:withdrawal_id])
      }
    end

    # @example
    #  {
    #    deposit_id: '177245',
    #    txid: '84f9e85bc5709cd471e3d58a7d0f42d2c4a7bbd888cabf844e200efbf0a7fda2',
    #    address: '1KK6HhG3quojFS4CY1mPcbyrjQ8BMDQxmT',
    #    amount: '0.13283',
    #    confirmations: 6,
    #    state: 2,
    #    created_at: '2014-01-31T22:01:30+01:00'
    #  }
    # @param [Hash] hde dados dum deposit
    # @return [Hash] deposit uniformizado
    def deposit_hash(hde)
      {
        id: hde[:address],
        tp: 'deposit',
        qtxt: 'btc',
        fee: '0',
        time: Time.parse(hde[:created_at]),
        qt: hde[:amount],
        lgid: Integer(hde[:deposit_id])
      }
    end

    private

    # HTTP GET request for public bitcoinde API queries.
    def api_get(uri, **ops)
      t = url("#{urb}/#{uri}", ops)
      resposta(Curl.get(t) { |r| r.headers = hdrs('GET', t, nonce, {}) })
    end

    # HTTP POST request for private bitcoinde API queries involving user credentials.
    def api_post(uri, **ops)
      # pedidos POST HTTP nao levam parametros no URL - somente no header e ordenados
      resposta(Curl.post("#{urb}/#{uri}") { |r| r.headers = hdrs('POST', "#{urb}/#{uri}", nonce, ops.sort) })
    end

    # @return [String] URL do pedido formatado com todos os parametros
    def url(uri, ops)
      ops.empty? ? uri : "#{uri}?#{URI.encode_www_form(ops)}"
    end

    # @return [Hash] headers necessarios para pedido HTTP
    def hdrs(typ, qry, non, ops)
      {
        'X-API-KEY': aky,
        'X-API-NONCE': non,
        'X-API-SIGNATURE': auth(typ, qry, non, URI.encode_www_form(ops))
      }
    end

    # @return [String] assinarura codificada dos pedidos HTTP
    def auth(typ, qry, non, par)
      raise(ArgumentError, 'API Key is not set') unless aky
      raise(ArgumentError, 'API Secret is not set') unless asc

      OpenSSL::HMAC.hexdigest('sha256', asc, [typ, qry, aky, non, Digest::MD5.hexdigest(par)].join('#'))
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
      "Erro da API bitcoinde #{e.inspect}"
    end
  end
end
