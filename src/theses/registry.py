from src.core.config import Settings
from src.theses.base import ThesisModule
from src.theses.economic_indicators.module import EconomicIndicatorsThesis
from src.theses.fda_trials.module import FdaTrialsThesis
from src.theses.sports_mlb.module import SportsMlbThesis
from src.theses.state_local_elections.module import StateLocalElectionsThesis
from src.theses.weather.module import WeatherThesis


def build_registry(settings: Settings) -> dict[str, ThesisModule]:
    return {
        "economic_indicators": EconomicIndicatorsThesis(settings),
        "state_local_elections": StateLocalElectionsThesis(),
        "weather": WeatherThesis(),
        "sports_mlb": SportsMlbThesis(),
        "fda_trials": FdaTrialsThesis(),
    }
