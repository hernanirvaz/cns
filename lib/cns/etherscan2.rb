# frozen_string_literal: true

# @author Hernani Rodrigues Vaz
module Cns
  # classe para processar transacoes do etherscan
  class Etherscan
    # @return [String] texto carteiras & transacoes & ajuste dias
    def mostra_resumo
      return unless dados.count.positive?

      puts("\nid     address             etherscan  tn ti tb tk  tw     bigquery  tn ti tb tk  tw")
      dados.each { |obj| puts(formata_carteira(obj)) }
      mostra_transacao_norml
      mostra_transacao_inter
      mostra_transacao_block
      mostra_transacao_token
      mostra_transacao_withw
      mostra_configuracao_ajuste_dias
    end

    # @param [Hash] hjn dados juntos bigquery & etherscan
    # @return [String] texto formatado duma carteira
    def formata_carteira(hjn)
      format(
        '%<s1>-6.6s %<s2>-16.16s ',
        s1: hjn[:id],
        s2: formata_enderec1(hjn[:ax], 16)
      ) + formata_valores(hjn)
    end

    # @param (see formata_carteira)
    # @return [String] texto formatado valores duma carteira
    def formata_valores(hjn)
      format(
        '%<v1>12.6f %<n1>3i %<n2>2i %<n3>2i %<n4>2i %<w1>3i %<v2>12.6f %<n5>3i %<n6>2i %<n7>2i %<n8>2i %<w2>3i %<ok>-3s',
        v1: hjn[:es],
        n1: hjn[:et].count,
        n2: hjn[:ei].count,
        n3: hjn[:ep].count,
        n4: hjn[:ek].count,
        w1: hjn[:ew].count,
        v2: hjn[:bs],
        n5: hjn[:bt].count,
        n6: hjn[:bi].count,
        n7: hjn[:bp].count,
        n8: hjn[:bk].count,
        w2: hjn[:bw].count,
        ok: ok?(hjn) ? 'OK' : 'NOK'
      )
    end

    # @param (see formata_carteira)
    # @return [Boolean] carteira tem transacoes novas(sim=NOK, nao=OK)?
    def ok?(hjn)
      hjn[:bs].round(6) == hjn[:es].round(6) && hjn[:bt].count == hjn[:et].count && hjn[:bi].count == hjn[:ei].count && hjn[:bp].count == hjn[:ep].count && hjn[:bk].count == hjn[:ek].count && hjn[:bw].count == hjn[:ew].count
    end

    # @example ether address inicio..fim
    #  0x10f3a0cf0b534c..c033cf32e8a03586
    # @param add (see filtrar_tx)
    # @param [Integer] max chars a mostrar
    # @return [String] endereco formatado
    def formata_enderec1(add, max)
      return 'erro' if max < 7

      max -= 2
      ini = Integer(max / 2) + 3
      inf = max % 2
      "#{add[0, ini - 3]}..#{add[-inf - ini - 3..]}"
    end

    # @example ether address inicio..fim
    #  me-app..4b437776403d
    # @param add (see filtrar_tx)
    # @param [Integer] max chars a mostrar
    # @return [String] endereco formatado
    def formata_enderec2(add, max)
      return 'erro' if max < 7

      max -= 2
      ini = Integer(max / 2)
      inf = max % 2
      hid = bqd[:wb].select { |obj| obj[:ax] == add }.first
      ndd = hid ? hid[:id] + '-' + add : add
      "#{ndd[0, ini - 3]}..#{ndd[-inf - ini - 3..]}"
    end

    # @example (see Apibc#norml_es)
    # @param [Hash] htx transacao normal etherscan
    # @return [String] texto formatado transacao normal etherscan
    def formata_transacao_norml(htx)
      format(
        '%<bn>9i %<fr>-20.20s %<to>-20.20s %<dt>10.10s %<vl>17.6f',
        bn: htx[:blockNumber],
        fr: formata_enderec2(htx[:from], 20),
        to: formata_enderec2(htx[:to], 20),
        dt: Time.at(Integer(htx[:timeStamp])),
        vl: (htx[:value].to_d / 10**18).round(10)
      )
    end

    # @example (see Apibc#block_es)
    # @param [Hash] htx transacao block etherscan
    # @return [String] texto formatado transacao block etherscan
    def formata_transacao_block(htx)
      format(
        '%<bn>9i %<fr>-41.41s %<dt>10.10s %<vl>17.6f',
        bn: htx[:blockNumber],
        fr: formata_enderec2(htx[:iax], 41),
        dt: Time.at(Integer(htx[:timeStamp])),
        vl: (htx[:blockReward].to_d / 10**18).round(10)
      )
    end

    # @example (see Apibc#token_es)
    # @param [Hash] hkx transacao token etherscan
    # @return [String] texto formatado transacao token etherscan
    def formata_transacao_token(hkx)
      format(
        '%<bn>9i %<fr>-20.20s %<to>-20.20s %<dt>10.10s %<vl>11.3f %<sy>-5.5s',
        bn: hkx[:blockNumber],
        fr: formata_enderec2(hkx[:from], 20),
        to: formata_enderec2(hkx[:to], 20),
        dt: Time.at(Integer(hkx[:timeStamp])),
        vl: (hkx[:value].to_d / 10**18).round(10),
        sy: hkx[:tokenSymbol]
      )
    end

    # @example (see Apibc#block_es)
    # @param [Hash] htx transacao withdrawals etherscan
    # @return [String] texto formatado transacao withdrawals etherscan
    def formata_transacao_withw(htx)
      format(
        '%<vi>9i %<bn>9i %<dt>10.10s %<vl>10.6f',
        vi: htx[:validatorIndex],
        bn: htx[:blockNumber],
        dt: Time.at(Integer(htx[:timestamp])),
        vl: (htx[:amount].to_d / 10**9).round(10)
      )
    end

    # @return [String] texto transacoes normais
    def mostra_transacao_norml
      return unless ops[:v] && novtx.count.positive?

      puts("\ntx normal from                 to                   data                   valor")
      sortx.each { |obj| puts(formata_transacao_norml(obj)) }
    end

    # @return [String] texto transacoes internas
    def mostra_transacao_inter
      return unless ops[:v] && novix.count.positive?

      puts("\ntx intern from                 to                   data                   valor")
      sorix.each { |obj| puts(formata_transacao_norml(obj)) }
    end

    # @return [String] texto transacoes block
    def mostra_transacao_block
      return unless ops[:v] && novpx.count.positive?

      puts("\ntx block  address                                   data                   valor")
      sorpx.each { |obj| puts(formata_transacao_block(obj)) }
    end

    # @return [String] texto transacoes token
    def mostra_transacao_token
      return unless ops[:v] && novkx.count.positive?

      puts("\ntx token  from                 to                   data             valor")
      sorkx.each { |obj| puts(formata_transacao_token(obj)) }
    end

    # @return [String] texto transacoes withdrawals
    def mostra_transacao_withw
      return unless ops[:v] && novwx.count.positive?

      puts("\nvalidator     block data            valor")
      sorwx.each { |obj| puts(formata_transacao_withw(obj)) }
    end

    # @return [String] texto configuracao ajuste dias das transacoes (normais & token)
    def mostra_configuracao_ajuste_dias
      return unless (novtx.count + novkx.count).positive?

      puts("\nstring ajuste dias\n-h=#{sorax.map { |obj| "#{obj[:blockNumber]}:0" }.join(' ')}")
    end
  end
end
