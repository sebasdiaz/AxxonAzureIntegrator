using Cronos;

namespace Axxon.Integrator.Core.Sync;

/// <summary>
/// Evaluación de las expresiones cron de <see cref="Model.MapSchedule"/>: formato
/// NCRONTAB de 5 o 6 campos (el mismo de los timer triggers de Functions), siempre en
/// UTC. El dispatcher es stateless: cada tick pregunta por la ventana desde el tick
/// anterior y la dedupe de la cola absorbe ventanas solapadas.
/// </summary>
public static class ScheduleEvaluator
{
    /// <summary>Valida la expresión; tira <see cref="FormatException"/> si no parsea (para el guardado del diseñador).</summary>
    public static void Validate(string cron)
    {
        try
        {
            Parse(cron);
        }
        catch (CronFormatException ex)
        {
            throw new FormatException($"Expresión cron inválida '{cron}': {ex.Message}", ex);
        }
    }

    /// <summary>
    /// Última ocurrencia del cron dentro de la ventana <c>(from, to]</c>, o null si no
    /// hay ninguna. Si el dispatcher viene rezagado y la ventana contiene varias
    /// ocurrencias, se dispara solo la última: un run agendado es incremental, correr
    /// dos veces seguidas no aporta nada.
    /// </summary>
    public static DateTimeOffset? LastDueInWindow(string cron, DateTimeOffset from, DateTimeOffset to)
    {
        var expression = Parse(cron);
        DateTimeOffset? last = null;
        foreach (var occurrence in expression.GetOccurrences(from.UtcDateTime, to.UtcDateTime, fromInclusive: false, toInclusive: true))
        {
            last = new DateTimeOffset(occurrence, TimeSpan.Zero);
        }
        return last;
    }

    private static CronExpression Parse(string cron) =>
        CronExpression.Parse(cron, cron.Trim().Split(' ', StringSplitOptions.RemoveEmptyEntries).Length == 6
            ? CronFormat.IncludeSeconds
            : CronFormat.Standard);
}
