# frozen_string_literal: true

require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  # classe para processar transacoes do greymass
  class Greymass
    # @return [Apibc] API blockchains
    # @return [Array<Hash>] todos os dados bigquery
    # @return [Thor::CoreExt::HashWithIndifferentAccess] opcoes trabalho
    attr_reader :api, :bqd, :ops

    TT = {sork: :itx, adjk: :itx}.freeze

    # @param [Hash] dad todos os dados bigquery
    # @param [Thor::CoreExt::HashWithIndifferentAccess] pop opcoes trabalho
    # @option pop [Hash] :h ({}) configuracao dias ajuste reposicionamento temporal
    # @option pop [Boolean] :v (false) mostra dados transacoes?
    # @option pop [Boolean] :t (false) mostra transacoes todas ou somente novas?
    def initialize(dad, pop)
      @bqd = dad
      @ops = pop.transform_keys(&:to_sym)
    end

    # Display summary of wallets, transactions, and adjustment days configuration
    def mresumo
      return unless dados.any?

      puts("\naddress            greymass  ntx       bigquery  ntx")
      dados.each { |e| puts(foct(e)) }
      mtransacoes_novas
      mconfiguracao_ajuste_dias
    end

    private

    # mosta transacoes novas
    def mtransacoes_novas
      return unless ops[:v] && novneost.any?

      puts("\nsequence num from         to           accao      data              valor moeda")
      novneost.sort_by { |s| -s[TT[:sork]] }.each { |t| puts(fol(t)) }
    end

    # mostra configuration text for adjusting days
    def mconfiguracao_ajuste_dias
      return unless novneost.any?

      puts("\nstring ajuste dias\n-h=#{novneost.sort_by { |s| -s[TT[:sork]] }.map { |t| "#{t[TT[:adjk]]}:0" }.join(' ')}")
    end

    # Format wallet summary text
    # @param [Hash] hjn dados juntos bigquery & greymass
    # @return [String] texto formatado duma carteira
    def foct(hjn)
      format(
        '%<address>-12.12s %<greymass_value>14.4f %<greymass_tx_count>4i %<bigquery_value>14.4f %<bigquery_tx_count>4i %<status>-3s',
        address: hjn[:ax],
        greymass_value: hjn[:es],
        greymass_tx_count: hjn[:et].count,
        bigquery_value: hjn[:bs],
        bigquery_tx_count: hjn[:bt].count,
        status: ok?(hjn) ? 'OK' : 'NOK'
      )
    end

    # Check if wallet has new transactions
    # @param (see foct)
    # @return [Boolean] carteira tem transacoes novas(sim=NOK, nao=OK)?
    def ok?(hjn)
      hjn[:bs].round(6) == hjn[:es].round(6) && hjn[:bt].count == hjn[:et].count
    end

    # Format transaction text
    # @param [Hash] hlx ledger greymass
    # @return [String] texto formatado
    def fol(hlx)
      format(
        '%<sequence>12i %<from>-12.12s %<to>-12.12s %<action>-10.10s %<date>10.10s %<value>12.4f %<symbol>-6.6s',
        sequence: hlx[:itx],
        from: hlx[:from],
        to: hlx[:to],
        action: hlx[:name],
        date: hlx[:block_time].strftime('%F'),
        value: hlx[:quantity],
        symbol: hlx[:moe]
      )
    end

    # Determine if all transactions should be shown
    # @return [Boolean] mostra todas/novas transacoes
    def show_all?
      ops[:t] || false
    end

    # Fetch EOS account resources
    # @param [String] add EOS account name
    # @return [Array<BigDecimal>] lista recursos - liquido, net, spu
    def peosa(add)
      hac = api.account_gm(add)
      htr = hac.fetch(:total_resources, {})
      [hac[:core_liquid_balance]&.to_d || 0.to_d, htr[:net_weight]&.to_d || 0.to_d, htr[:cpu_weight]&.to_d || 0.to_d]
    end

    # Process and filter EOS transactions
    # @param add (see peosa)
    # @param [Array<Hash>] ary lista transacoes
    # @return [Array<Hash>] lista transacoes filtrada
    def peost(add, ary)
      ary.map do |t|
        act = t[:action_trace][:act]
        adt = act[:data]
        qtd = adt[:quantity].to_s
        t.merge(
          name: act[:name],
          from: adt[:from],
          quantity: qtd.to_d,
          account: act[:account],
          to: adt[:to],
          memo: adt[:memo].to_s.gsub(/\p{C}/, ''), # remove Non-Printable Characters
          moe: qtd[/[[:upper:]]+/],
          itx: t[:global_action_seq],
          iax: add,
          block_time: Time.parse(t[:block_time])
        )
      end
    end

    # Fetch Greymass data for a wallet
    # @param [Hash] wbq wallet bigquery
    # @return [Hash] dados greymass - address, saldo & transacoes
    def bsgm(wbq)
      xbq = wbq[:ax]
      {ax: xbq, sl: peosa(xbq).reduce(:+), tx: peost(xbq, api.ledger_gm(xbq))}
    end

    # Combine BigQuery and Greymass data
    # @param wbq (see bsgm)
    # @param [Hash] hgm dados greymass - address, saldo & transacoes
    # @return [Hash] dados juntos bigquery & greymass
    def bqgm(wbq, hgm)
      xbq = wbq[:ax]
      {
        id: wbq[:id],
        ax: xbq,
        bs: wbq[:sl],
        bt: bqd[:nt].select { |o| o[:iax] == xbq },
        es: hgm[:sl],
        et: hgm[:tx]
      }
    end

    # Lazy Greymass API Initialization
    # @return [Apibc] API instance
    def api
      @api ||= Apibc.new
    end

    # Fetch all Greymass data
    # @return [Hash] Hash of Greymass data indexed by address
    def gmd
      @gmd ||= bqd[:wb].map { |o| bsgm(o) }.each_with_object({}) { |h, a| a[h[:ax]] = h }
    end

    # Fetch combined BigQuery and Greymass data
    # @return [Array<Hash>] Combined data list
    def dados
      @dados ||= bqd[:wb].map { |b| bqgm(b, gmd[b[:ax]]) }
    end

    # @return [Array<Integer>] indices transacoes bigquery
    def bqidt
      @bqidt ||= show_all? ? [] : bqd[:nt].map { |i| i[:itx] }
    end

    # @return [Array<Integer>] indices transacoes novas (greymass - bigquery)
    def idt
      @idt ||= gmd.values.map { |o| o[:tx].map { |i| i[:itx] } }.flatten - bqidt
    end

    # Get new transactions
    # @return [Array<Hash>] List of new transactions
    def novneost
      @novneost ||= gmd.values.map { |t| t[:tx].select { |o| idt.include?(o[:itx]) } }.flatten.uniq { |i| i[:itx] }
    end
  end
end
