using Axxon.Integrator.Core.Sync;
using Xunit;

namespace Axxon.Integrator.Tests;

/// <summary>
/// Evaluación de crons de mapas agendados: 5 o 6 campos NCRONTAB en UTC, ventana
/// (from, to] del dispatcher stateless, y solo la última ocurrencia si hay varias.
/// </summary>
public sealed class ScheduleEvaluatorTests
{
    private static readonly DateTimeOffset Noon = new(2026, 7, 16, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void Six_field_cron_fires_inside_window()
    {
        var due = ScheduleEvaluator.LastDueInWindow("0 */15 * * * *", Noon.AddSeconds(30), Noon.AddMinutes(15).AddSeconds(30));

        Assert.Equal(Noon.AddMinutes(15), due);
    }

    [Fact]
    public void Five_field_cron_is_accepted()
    {
        var due = ScheduleEvaluator.LastDueInWindow("*/5 * * * *", Noon.AddMinutes(2), Noon.AddMinutes(6));

        Assert.Equal(Noon.AddMinutes(5), due);
    }

    [Fact]
    public void Multiple_occurrences_in_window_collapse_to_the_last()
    {
        // dispatcher rezagado: correr una sola vez alcanza, el run incremental se pone al día solo
        var due = ScheduleEvaluator.LastDueInWindow("0 */15 * * * *", Noon, Noon.AddHours(1));

        Assert.Equal(Noon.AddHours(1), due);
    }

    [Fact]
    public void Window_start_is_exclusive_and_no_match_returns_null()
    {
        Assert.Null(ScheduleEvaluator.LastDueInWindow("0 0 3 * * *", Noon, Noon.AddMinutes(1)));
        // la ocurrencia exacta del borde izquierdo pertenece a la ventana anterior
        Assert.Null(ScheduleEvaluator.LastDueInWindow("0 0 12 * * *", Noon, Noon.AddSeconds(59)));
    }

    [Fact]
    public void Invalid_cron_throws_format_exception()
    {
        Assert.Throws<FormatException>(() => ScheduleEvaluator.Validate("cada 15 minutos"));
        Assert.Throws<FormatException>(() => ScheduleEvaluator.Validate("99 * * * *"));
    }

    [Fact]
    public void Valid_cron_passes_validation()
    {
        ScheduleEvaluator.Validate("0 */15 * * * *");
        ScheduleEvaluator.Validate("30 2 * * *");
    }
}
