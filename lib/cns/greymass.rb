# frozen_string_literal: true

require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  # classe para processar transacoes do greymass
  class Greymass
    # @return [Apibc] API blockchains
    attr_reader :api
    # @return [Array<Hash>] todos os dados bigquery
    attr_reader :bqd
    # @return [Thor::CoreExt::HashWithIndifferentAccess] opcoes trabalho
    attr_reader :ops

    # @param [Hash] dad todos os dados bigquery
    # @param [Thor::CoreExt::HashWithIndifferentAccess] pop opcoes trabalho
    # @option pop [Hash] :h ({}) configuracao dias ajuste reposicionamento temporal
    # @option pop [Boolean] :v (false) mostra dados transacoes?
    # @option pop [Boolean] :t (false) mostra transacoes todas ou somente novas?
    # @return [Greymass] API greymass - processar transacoes
    def initialize(dad, pop)
      @api = Apibc.new
      @bqd = dad
      @ops = pop.transform_keys(&:to_sym)
    end

    TT = {
      new: :novneost,
      format: :formata_ledger,
      header: "\nsequence num from         to           accao      data              valor moeda",
      sork: :itx,
      adjk: :itx
    }

    # @return [String] texto carteiras & transacoes & ajuste dias
    def mresumo
      return unless dados.any?

      puts("\naddress            greymass  ntx       bigquery  ntx")
      dados.each { |obj| puts(formata_carteira(obj)) }
      mtransacoes_novas
      mconfiguracao_ajuste_dias
    end

    private

    # @param [Hash] hjn dados juntos bigquery & greymass
    # @return [String] texto formatado duma carteira
    def formata_carteira(hjn)
      format(
        '%<s1>-12.12s %<v1>14.4f %<n1>4i %<v2>14.4f %<n2>4i %<ok>-3s',
        s1: hjn[:ax],
        v1: hjn[:es],
        n1: hjn[:et].count,
        v2: hjn[:bs],
        n2: hjn[:bt].count,
        ok: ok?(hjn) ? 'OK' : 'NOK'
      )
    end

    # @param (see formata_carteira)
    # @return [Boolean] carteira tem transacoes novas(sim=NOK, nao=OK)?
    def ok?(hjn)
      hjn[:bs] == hjn[:es] && hjn[:bt].count == hjn[:et].count
    end

    # @example (see Apibc#ledger_gm)
    # @param [Hash] hlx ledger greymass
    # @return [String] texto formatado ledger greymass
    def formata_ledger(hlx)
      format(
        '%<bn>12i %<fr>-12.12s %<to>-12.12s %<ac>-10.10s %<dt>10.10s %<vl>12.4f %<sy>-6.6s',
        ac: hlx[:name],
        fr: hlx[:from],
        vl: hlx[:quantity],
        bn: hlx[:itx],
        to: hlx[:to],
        dt: hlx[:block_time].strftime('%F'),
        sy: hlx[:moe]
      )
    end

    # @return [String] Display new transactions based on verbose option
    def mtransacoes_novas
      ntx = send(TT[:new])
      return unless ops[:v] && ntx.any?

      puts(TT[:header])
      ntx.sort_by { |s| -s[TT[:sork]] }.each { |t| puts(send(TT[:format], t)) }
    end

    # @return [String] texto configuracao ajuste dias das transacoes
    def mconfiguracao_ajuste_dias
      ntx = send(TT[:new])
      return unless ntx.any?

      puts("\nstring ajuste dias\n-h=#{ntx.sort_by { |s| -s[TT[:sork]] }.map { |t| "#{t[TT[:adjk]]}:0" }.join(' ')}")
    end

    # @return [Array<Hash>] todos os dados greymass - saldos & transacoes
    def bcd
      @bcd ||= bqd[:wb].map { |obj| base_bc(obj) }
    end

    # @return [Array<Hash>] todos os dados juntos bigquery & greymass
    def dados
      @dados ||= bqd[:wb].map { |b| bq_bc(b, bcd.find { |g| b[:ax] == g[:ax] }) }
    end

    def show_all?
      ops[:t] || false
    end

    def bqidt
      @bqidt ||= show_all? ? [] : (bqd[:nt]&.map { |i| i[:itx] } || [])
    end

    # @return [Array<Integer>] lista indices transacoes novas
    def idt
      @idt ||= bcd.map { |o| o[:tx].map { |i| i[:itx] } }.flatten - bqidt
    end

    # @example (see Apibc#account_gm)
    # @param [Hash] wbq wallet bigquery
    # @return [Hash] dados greymass - address, saldo & transacoes
    def base_bc(wbq)
      xbq = wbq[:ax]
      {ax: xbq, sl: peosa(xbq).reduce(:+), tx: peost(xbq, api.ledger_gm(xbq))}
    end

    # @param wbq (see base_bc)
    # @param [Hash] hbc dados greymass - address, saldo & transacoes
    # @return [Hash] dados juntos bigquery & greymass
    def bq_bc(wbq, hbc)
      xbq = wbq[:ax]
      {
        id: wbq[:id],
        ax: xbq,
        bs: wbq[:sl],
        bt: bqd[:nt].select { |obj| obj[:iax] == xbq },
        es: hbc[:sl],
        et: hbc[:tx]
      }
    end

    # @return [Array<Hash>] lista transacoes novas
    def novneost
      @novneost ||= bcd.map { |obc| obc[:tx].select { |obj| idt.include?(obj[:itx]) } }.flatten
    end

    # @return [Array<BigDecimal>] lista recursos - liquido, net, spu
    def peosa(add)
      hac = api.account_gm(add)
      htr = hac[:total_resources]
      [hac[:core_liquid_balance].to_d, htr[:net_weight].to_d, htr[:cpu_weight].to_d]
    end

    # @param add (see Apibc#account_gm)
    # @param [Array<Hash>] ary lista transacoes
    # @return [Array<Hash>] lista transacoes filtrada
    def peost(add, ary)
      ary.map do |omp|
        act = omp[:action_trace][:act]
        adt = act[:data]
        qtd = adt[:quantity].to_s
        omp.merge(
          name: act[:name],
          from: adt[:from],
          quantity: qtd.to_d,
          account: act[:account],
          to: adt[:to],
          memo: String(adt[:memo]).gsub(/\p{C}/, ''), # remove Non-Printable Characters
          moe: qtd[/[[:upper:]]+/],
          itx: omp[:global_action_seq],
          iax: add,
          block_time: Time.parse(omp[:block_time])
        )
      end
    end
  end
end
