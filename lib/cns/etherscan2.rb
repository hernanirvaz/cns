# frozen_string_literal: true

# @author Hernani Rodrigues Vaz
module Cns
  # classe para processar transacoes do etherscan
  class Etherscan
    # @return [String] texto carteiras & transacoes & ajuste dias
    def mostra_resumo
      return unless dados.count.positive?

      puts("\nid     address                            etherscan nm tk     bigquery nm tk")
      dados.each { |e| puts(formata_carteira(e)) }
      mostra_transacao_norml
      mostra_transacao_token
      mostra_configuracao_ajuste_dias
    end

    # @param [Hash] hjn dados juntos bigquery & etherscan
    # @return [String] texto formatado duma carteira
    def formata_carteira(hjn)
      format(
        '%<s1>-6.6s %<s2>-32.32s ',
        s1: hjn[:id],
        s2: formata_endereco(hjn[:ax], 32)
      ) + formata_valores(hjn)
    end

    # @param (see formata_carteira)
    # @return [String] texto formatado valores duma carteira
    def formata_valores(hjn)
      format(
        '%<v1>11.6f %<n1>2i %<n3>2i %<v2>12.6f %<n2>2i %<n4>2i %<ok>-3s',
        v1: hjn[:es],
        n1: hjn[:et].count,
        n3: hjn[:ek].count,
        v2: hjn[:bs],
        n2: hjn[:bt].count,
        n4: hjn[:bk].count,
        ok: ok?(hjn) ? 'OK' : 'NOK'
      )
    end

    # @param (see formata_carteira)
    # @return [Boolean] carteira tem transacoes novas(sim=NOK, nao=OK)?
    def ok?(hjn)
      hjn[:bs] == hjn[:es] && hjn[:bt].count == hjn[:et].count && hjn[:bk].count == hjn[:ek].count
    end

    # @example ether address inicio..fim
    #  0x10f3a0cf0b534c..c033cf32e8a03586
    # @param add (see filtrar_tx)
    # @param [Integer] max chars a mostrar
    # @return [String] endereco formatado
    def formata_endereco(add, max)
      i = Integer((max - 2) / 2)
      e = (max <= 20 ? bqd[:wb].select { |s| s[:ax] == add }.first : nil) || { id: add }
      max < 7 ? 'erro' : "#{e[:id][0, i - 3]}..#{add[-i - 3..]}"
    end

    # @example (see Apibc#norml_es)
    # @param [Hash] htx transacao normal etherscan
    # @return [String] texto formatado transacao normal etherscan
    def formata_transacao_norml(htx)
      format(
        '%<bn>9i %<fr>-20.20s %<to>-20.20s %<dt>10.10s %<vl>17.6f',
        bn: htx[:blockNumber],
        fr: formata_endereco(htx[:from], 20),
        to: formata_endereco(htx[:to], 20),
        dt: Time.at(Integer(htx[:timeStamp])),
        vl: (htx[:value].to_d / 10**18).round(10)
      )
    end

    # @example (see Apibc#token_es)
    # @param [Hash] hkx transacao token etherscan
    # @return [String] texto formatado transacao token etherscan
    def formata_transacao_token(hkx)
      format(
        '%<bn>9i %<fr>-20.20s %<to>-20.20s %<dt>10.10s %<vl>11.3f %<sy>-5.5s',
        bn: hkx[:blockNumber],
        fr: formata_endereco(hkx[:from], 20),
        to: formata_endereco(hkx[:to], 20),
        dt: Time.at(Integer(hkx[:timeStamp])),
        vl: (hkx[:value].to_d / 10**18).round(10),
        sy: hkx[:tokenSymbol]
      )
    end

    # @return [String] texto transacoes normais
    def mostra_transacao_norml
      return unless ops[:v] && novtx.count.positive?

      puts("\ntx normal from                 to                   data                   valor")
      sortx.each { |e| puts(formata_transacao_norml(e)) }
    end

    # @return [String] texto transacoes token
    def mostra_transacao_token
      return unless ops[:v] && novkx.count.positive?

      puts("\ntx token  from                 to                   data             valor")
      sorkx.each { |e| puts(formata_transacao_token(e)) }
    end

    # @return [String] texto configuracao ajuste dias das transacoes (normais & token)
    def mostra_configuracao_ajuste_dias
      return unless (novtx.count + novkx.count).positive?

      puts("\nstring ajuste dias\n-h=#{sorax.map { |e| "#{e[:blockNumber]}:0" }.join(' ')}")
    end
  end
end
