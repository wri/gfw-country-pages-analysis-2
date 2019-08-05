class LayerFactory:
    def __init__(self):
        self._layers = {}

    def register_layer(self, name, layer):
        self._layers[name] = layer

    def get_layer(self, name, env):
        layer = self._layers.get(name)
        if not layer:
            raise ValueError(format)
        return layer(env)
