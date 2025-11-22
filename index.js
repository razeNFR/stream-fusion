import { FluxDispatcher } from "@vendetta/metro/common";

export default {
  onLoad() {
    const setInvisible = () => {
      FluxDispatcher.dispatch({
        type: "LOCAL_STATUS_UPDATE",
        status: "invisible"
      });
    };

    // Appliquer invisible au démarrage
    setInvisible();

    // Réappliquer si l’app change d’état
    FluxDispatcher.subscribe("APP_STATE_UPDATE", setInvisible);
  },

  onUnload() {
    // Nettoyage si besoin
  }
};
