# frozen_string_literal: true

require('faraday')
require('json')

module Cns
  # classe para acesso dados blockchain EOS
  class Apigm
    # @return [String] base URL to use as a prefix for all requests
    attr_reader :url

    # @param [String] www base URL to use as a prefix for all requests
    # @return [Apigm] acesso dados blockchain EOS
    def initialize(www: 'https://eos.greymass.com')
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
    #  {
    #    account_name: '...',
    #    head_block_num: 141_391_122,
    #    head_block_time: '2020-09-11T16:05:51.000',
    #    privileged: false,
    #    last_code_update: '1970-01-01T00:00:00.000',
    #    created: '2018-06-09T13:14:37.000',
    #    core_liquid_balance: '1232.0228 EOS',
    #    ram_quota: 9548,
    #    net_weight: 10_001_142,
    #    cpu_weight: 10_001_144,
    #    net_limit: { used: 0, available: 1_066_648_346, max: 1_066_648_346 },
    #    cpu_limit: { used: 338, available: 88_498, max: 88_836 },
    #    ram_usage: 3574,
    #    permissions: [
    #      {
    #        perm_name: 'active',
    #        parent: 'owner',
    #        required_auth: {
    #          threshold: 1, keys: [{ key: 'EOS...', weight: 1 }], accounts: [], waits: []
    #        }
    #      },
    #      {
    #        perm_name: 'owner',
    #        parent: '',
    #        required_auth: {
    #          threshold: 1, keys: [{ key: 'EOS...', weight: 1 }], accounts: [], waits: []
    #        }
    #      }
    #    ],
    #    total_resources: { owner: '...', net_weight: '1000.1142 EOS', cpu_weight: '1000.1144 EOS', ram_bytes: 8148 },
    #    self_delegated_bandwidth: { from: '...', to: '...', net_weight: '1000.1142 EOS', cpu_weight: '1000.1144 EOS' },
    #    refund_request: nil,
    #    voter_info: {
    #      owner: '...',
    #      proxy: '...',
    #      producers: [],
    #      staked: 20_002_286,
    #      last_vote_weight: '17172913021904.12109375000000000',
    #      proxied_vote_weight: '0.00000000000000000',
    #      is_proxy: 0,
    #      flags1: 0,
    #      reserved2: 0,
    #      reserved3: '0.0000 EOS'
    #    },
    #    rex_info: nil
    #  }
    # @param [Hash] arg argumentos trabalho
    # @option arg [String] :account_name endereco carteira EOS
    # @return [Hash] dados & saldo duma carteira EOS
    def account(**arg)
      raise(Erro, 'endereco tem de ser definido') if arg[:account_name].nil? || arg[:account_name].empty?

      get('/v1/chain/get_account', **arg)
    end

    # @example
    #  {
    #    actions: [
    #      {
    #        account_action_seq: 964,
    #        action_trace: {
    #          account_ram_deltas: [],
    #          act: {
    #            account: 'voicebestapp',
    #            authorization: [
    #              { actor: 'thetruevoice', permission: 'active' },
    #              { actor: 'voicebestapp', permission: 'active' }
    #            ],
    #            data: { from: 'voicebestapp', memo: '...', quantity: '1.0001 MESSAGE', to: '...' },
    #            hex_data: '...',
    #            name: 'transfer'
    #          },
    #          action_ordinal: 10,
    #          block_num: 141_345_345,
    #          block_time: '2020-09-11T09:44:04.500',
    #          closest_unnotified_ancestor_action_ordinal: 5,
    #          context_free: false,
    #          creator_action_ordinal: 5,
    #          elapsed: 6,
    #          producer_block_id: '...',
    #          receipt: {
    #            abi_sequence: 1,
    #            act_digest: '...',
    #            auth_sequence: [['thetruevoice', 6_778_215], ['voicebestapp', 435_346]],
    #            code_sequence: 1,
    #            global_sequence: 233_283_589_258,
    #            receiver: '...',
    #            recv_sequence: 927
    #          },
    #          receiver: '...',
    #          trx_id: '...'
    #        },
    #        block_num: 141_345_345,
    #        block_time: '2020-09-11T09:44:04.500',
    #        global_action_seq: 233_283_589_258,
    #        irreversible: true
    #      },
    #      {}
    #    ],
    #    head_block_num: 141_721_698,
    #    last_irreversible_block: 141_721_371
    #  }
    # @param [String] add endereco carteira EOS
    # @param [Hash] arg argumentos trabalho
    # @option arg [String] :account_name endereco carteira EOS
    # @option arg [Integer] :pos posicao da primeira transacao a devolver
    # @option arg [Integer] :offset numero maximo transacoes a devolver
    # @option arg [String] :filter filtro a aplicar na resposta
    # @option arg [String] :sort ordenacao asc/desc
    # @option arg [String] :after time inicio "2020-09-13T13:44:03.105Z"
    # @option arg [String] :before time fim   "2020-09-13T13:44:03.105Z"
    # @option arg [Integer] :parent transacao pai
    # @return [Array<Hash>] devolve lista de transacoes
    def all_tx(add, **arg)
      raise(Erro, 'endereco tem de ser definido') if add.nil? || add.empty?

      ledger(**arg.merge(account_name: add))
    end

    # @param [Integer] pos posicao das transacoes a devolver
    # @param [Array<Hash>] ary lista acumuladora das transacoes a devolver
    # @param arg (see all_tx)
    # @option arg (see all_tx)
    # @return [Array<Hash>] lista das transacoes ligadas a uma carteira EOS
    def ledger(pos = 0, ary = [], **arg)
      r = get('/v1/history/get_actions', **arg.merge(pos: pos, offset: 100))[:actions]
      ary += r
      r.count < 100 ? ary : ledger(pos + r.count, ary, **arg)
    rescue StandardError
      ary
    end

    private

    # @param [String] uri identificacao do recurso a questionar
    # @param arg (see all_tx)
    # @option arg (see all_tx)
    # @return [Hash] resultado do HTTP request
    def get(uri, **arg)
      JSON.parse(
        conn.post(uri, arg.to_json, content_type: 'application/json').body,
        symbolize_names: true
      )
    rescue StandardError
      { actions: [] }
    end
  end
end
