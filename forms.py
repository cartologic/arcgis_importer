from django import forms

from .models import ArcGISLayerImport


class ArcGISLayerImportForm(forms.ModelForm):
    class Meta:
        model = ArcGISLayerImport
        fields = ['url']
        widgets = {
            'url': forms.TextInput(attrs={
                'id': 'layer-url',
                'required': True,
                'placeholder': 'Esri Layer URL...'
            }),
        }
