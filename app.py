"""Point d'entrée pour Vercel (Flask détecté automatiquement)."""
from interface.app import app  # noqa: F401

if __name__ == "__main__":
    import os
    app.run(host="127.0.0.1", port=int(os.getenv("PORT", "5000")), debug=False)
