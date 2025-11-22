module.exports = {
    start() {
        const { Dispatcher } = require("discord-revenge");

        function setInvisible() {
            Dispatcher.dispatch({
                type: "USER_SETTINGS_UPDATE",
                status: "invisible"
            });
        }

        // Appliquer invisible dès le lancement
        setInvisible();

        // Réappliquer si l'app change d'état
        Dispatcher.subscribe("APP_STATE_UPDATE", () => {
            setInvisible();
        });
    },

    stop() {
        // Rien à nettoyer, mais tu pourrais retirer des listeners si besoin
    }
};
