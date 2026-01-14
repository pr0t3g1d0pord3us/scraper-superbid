#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SUPABASE CLIENT - ATUALIZADO
✅ Estrutura simplificada: has_bid (boolean) + auction_round
❌ Removido: total_bids, total_bidders, total_visits, days_remaining
"""

import os
import time
import requests
from datetime import datetime


class SupabaseClient:
    """Cliente para Supabase - Schema auctions (simplificado)"""
    
    def __init__(self):
        self.url = os.getenv('SUPABASE_URL')
        self.key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
        
        if not self.url or not self.key:
            raise ValueError("❌ Configure SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY")
        
        self.url = self.url.rstrip('/')
        
        self.headers = {
            'apikey': self.key,
            'Authorization': f'Bearer {self.key}',
            'Content-Type': 'application/json',
            'Content-Profile': 'auctions',
            'Accept-Profile': 'auctions',
            'Prefer': 'resolution=merge-duplicates,return=minimal'
        }
        
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def upsert(self, tabela: str, items: list) -> dict:
        """Upsert com estrutura simplificada"""
        if not items:
            return {'inserted': 0, 'updated': 0, 'errors': 0}
        
        prepared = []
        for item in items:
            try:
                db_item = self._prepare(item, tabela)
                if db_item:
                    prepared.append(db_item)
            except Exception as e:
                print(f"  ⚠️ Erro ao preparar item: {e}")
        
        if not prepared:
            print("  ⚠️ Nenhum item válido para inserir")
            return {'inserted': 0, 'updated': 0, 'errors': 0}
        
        # Normaliza chaves do batch
        prepared = self._normalize_batch_keys(prepared, tabela)
        
        stats = {'inserted': 0, 'updated': 0, 'errors': 0}
        batch_size = 500
        total_batches = (len(prepared) + batch_size - 1) // batch_size
        
        url = f"{self.url}/rest/v1/{tabela}"
        
        for i in range(0, len(prepared), batch_size):
            batch = prepared[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            
            try:
                r = self.session.post(url, json=batch, timeout=120)
                
                if r.status_code in (200, 201):
                    stats['inserted'] += len(batch)
                    print(f"  ✅ Batch {batch_num}/{total_batches}: {len(batch)} itens")
                elif r.status_code == 409:
                    stats['updated'] += len(batch)
                    print(f"  🔄 Batch {batch_num}/{total_batches}: {len(batch)} atualizados")
                else:
                    error_msg = r.text[:200] if r.text else 'Sem detalhes'
                    print(f"  ❌ Batch {batch_num}: HTTP {r.status_code} - {error_msg}")
                    stats['errors'] += len(batch)
            
            except Exception as e:
                print(f"  ❌ Batch {batch_num}: {e}")
                stats['errors'] += len(batch)
            
            if batch_num < total_batches:
                time.sleep(0.5)
        
        return stats
    
    def _normalize_batch_keys(self, items: list, tabela: str = '') -> list:
        """Garante que todos os itens tenham apenas campos válidos"""
        if not items:
            return items
        
        # ✅ CAMPOS PADRÃO - ESTRUTURA SIMPLIFICADA
        standard_fields = {
            'source', 'external_id', 'title', 'normalized_title', 'description_preview',
            'description', 'value', 'value_text', 'city', 'state', 'address',
            'auction_date', 'auction_type', 'auction_name',
            'store_name', 'lot_number', 
            'link', 'metadata', 'duplicate_group', 'is_primary_duplicate',
            'is_active', 'created_at', 'updated_at', 'last_scraped_at',
            
            # ✅ SIMPLIFICADO
            'has_bid',  # Boolean ao invés de contadores
            'auction_round',  # NULL = 1ª praça, 2 = 2ª praça
        }
        
        # Campos específicos por tabela
        table_specific_fields = {
            'veiculos': {'vehicle_type'},
            'imoveis': {'property_type'},
            'animais': {'animal_type'},
            'tecnologia': {'multiplecategory', 'tech_type'},
            'bens_consumo': {'consumption_goods_type'},
            'partes_pecas': {'parts_type'},
            'nichados': {'specialized_type'},
            'eletrodomesticos': {'appliance_type'},
            'materiais_construcao': {'construction_material_type'},
        }
        
        # Campos permitidos
        allowed_fields = standard_fields.copy()
        if tabela in table_specific_fields:
            allowed_fields.update(table_specific_fields[tabela])
        
        # Coleta chaves válidas
        all_keys = set()
        for item in items:
            for key in item.keys():
                if key in allowed_fields:
                    all_keys.add(key)
        
        # Normaliza itens
        normalized = []
        for item in items:
            normalized_item = {}
            for key in all_keys:
                normalized_item[key] = item.get(key, None)
            normalized.append(normalized_item)
        
        return normalized
    
    def _prepare(self, item: dict, tabela: str = '') -> dict:
        """Prepara item para inserção no banco"""
        source = item.get('source')
        external_id = item.get('external_id')
        title = item.get('title') or 'Sem Título'
        
        if not source or not external_id:
            return None
        
        # Processa auction_date
        auction_date = item.get('auction_date')
        if auction_date and isinstance(auction_date, str):
            try:
                auction_date = auction_date.replace('Z', '+00:00')
                dt = datetime.fromisoformat(auction_date)
                auction_date = dt.isoformat()
            except:
                auction_date = None
        
        # Valida state
        state = item.get('state')
        if state:
            state = str(state).strip().upper()
            if len(state) != 2:
                state = None
        
        # Processa value
        value = item.get('value')
        if value is not None:
            try:
                value = float(value)
                if value < 0:
                    value = None
            except:
                value = None
        
        # ✅ HAS_BID - Converte para boolean
        has_bid = item.get('has_bid')
        if has_bid is not None:
            if isinstance(has_bid, bool):
                has_bid = has_bid
            elif isinstance(has_bid, (int, float)):
                has_bid = has_bid > 0
            elif isinstance(has_bid, str):
                has_bid = has_bid.lower() in ('true', '1', 'yes', 'sim')
            else:
                has_bid = False
        else:
            has_bid = False
        
        # ✅ AUCTION_ROUND - NULL ou 2
        auction_round = item.get('auction_round')
        if auction_round is not None:
            try:
                auction_round = int(auction_round)
                # Aceita apenas 2 (segunda praça)
                auction_round = 2 if auction_round == 2 else None
            except:
                auction_round = None
        
        metadata = item.get('metadata', {})
        if not isinstance(metadata, dict):
            metadata = {}
        
        # ✅ CAMPOS PADRÃO - ESTRUTURA SIMPLIFICADA
        data = {
            'source': str(source),
            'external_id': str(external_id),
            'title': str(title)[:255],
            'normalized_title': str(item.get('normalized_title') or title)[:255],
            'description_preview': str(item.get('description_preview', ''))[:255] if item.get('description_preview') else None,
            'description': str(item.get('description')) if item.get('description') else None,
            'value': value,
            'value_text': str(item.get('value_text')) if item.get('value_text') else None,
            'city': str(item.get('city')) if item.get('city') else None,
            'state': state,
            'address': str(item.get('address')) if item.get('address') else None,
            'auction_date': auction_date,
            'auction_type': str(item.get('auction_type', 'Leilão'))[:100],
            'auction_name': str(item.get('auction_name')) if item.get('auction_name') else None,
            'store_name': str(item.get('store_name')) if item.get('store_name') else None,
            'lot_number': str(item.get('lot_number')) if item.get('lot_number') else None,
            'link': str(item.get('link')) if item.get('link') else None,
            'metadata': metadata,
            'is_active': True,
            'last_scraped_at': datetime.now().isoformat(),
            
            # ✅ SIMPLIFICADO
            'has_bid': has_bid,  # Boolean
            'auction_round': auction_round,  # NULL ou 2
        }
        
        # ✅ Campos específicos por tabela
        if tabela == 'veiculos':
            vehicle_type = item.get('vehicle_type')
            if not vehicle_type and isinstance(metadata, dict):
                vehicle_type = metadata.get('vehicle_type')
            
            if vehicle_type:
                data['vehicle_type'] = str(vehicle_type)[:255]
        
        if tabela == 'imoveis':
            property_type = item.get('property_type')
            if not property_type and isinstance(metadata, dict):
                property_type = metadata.get('property_type')
            
            if property_type:
                data['property_type'] = str(property_type)[:255]
        
        if tabela == 'animais':
            animal_type = item.get('animal_type')
            if not animal_type and isinstance(metadata, dict):
                animal_type = metadata.get('animal_type')
            
            if animal_type:
                data['animal_type'] = str(animal_type)[:255]
        
        if tabela == 'tecnologia':
            multiplecategory = item.get('multiplecategory')
            if not multiplecategory and isinstance(metadata, dict):
                multiplecategory = metadata.get('multiplecategory')
            
            if multiplecategory and isinstance(multiplecategory, list):
                data['multiplecategory'] = multiplecategory
            
            tech_type = item.get('tech_type')
            if not tech_type and isinstance(metadata, dict):
                tech_type = metadata.get('tech_type')
            
            if tech_type:
                data['tech_type'] = str(tech_type)[:255]
        
        if tabela == 'bens_consumo':
            consumption_goods_type = item.get('consumption_goods_type')
            if not consumption_goods_type and isinstance(metadata, dict):
                consumption_goods_type = metadata.get('consumption_goods_type')
            
            if consumption_goods_type:
                data['consumption_goods_type'] = str(consumption_goods_type)[:255]
        
        if tabela == 'partes_pecas':
            parts_type = item.get('parts_type')
            if not parts_type and isinstance(metadata, dict):
                parts_type = metadata.get('parts_type')
            
            if parts_type:
                data['parts_type'] = str(parts_type)[:255]
        
        if tabela == 'nichados':
            specialized_type = item.get('specialized_type')
            if not specialized_type and isinstance(metadata, dict):
                specialized_type = metadata.get('specialized_type')
            
            if specialized_type:
                data['specialized_type'] = str(specialized_type)[:255]
        
        if tabela == 'eletrodomesticos':
            appliance_type = item.get('appliance_type')
            if not appliance_type and isinstance(metadata, dict):
                appliance_type = metadata.get('appliance_type')
            
            if appliance_type:
                data['appliance_type'] = str(appliance_type)[:255]
        
        if tabela == 'materiais_construcao':
            construction_material_type = item.get('construction_material_type')
            if not construction_material_type and isinstance(metadata, dict):
                construction_material_type = metadata.get('construction_material_type')
            
            if construction_material_type:
                data['construction_material_type'] = str(construction_material_type)[:255]
        
        return data
    
    def save_bid_history_snapshot(self, category: str, items: list) -> dict:
        """
        ✅ SALVA SNAPSHOT NO HISTÓRICO
        
        Estrutura simplificada:
        - category
        - external_id
        - has_bid (boolean)
        - current_value
        - captured_at
        """
        if not items:
            return {'saved': 0, 'errors': 0}
        
        snapshots = []
        
        for item in items:
            try:
                snapshot = {
                    'category': category,
                    'source': item.get('source'),
                    'external_id': item.get('external_id'),
                    'lot_number': item.get('lot_number'),
                    'has_bid': bool(item.get('has_bid', False)),  # ✅ Boolean
                    'current_value': float(item.get('value')) if item.get('value') else None,
                    'captured_at': datetime.now().isoformat(),
                }
                
                if snapshot['external_id']:
                    snapshots.append(snapshot)
            
            except Exception as e:
                print(f"  ⚠️ Erro ao criar snapshot: {e}")
        
        if not snapshots:
            return {'saved': 0, 'errors': 0}
        
        # Insert no histórico
        stats = {'saved': 0, 'errors': 0}
        url = f"{self.url}/rest/v1/auction_bid_history"
        
        batch_size = 1000
        for i in range(0, len(snapshots), batch_size):
            batch = snapshots[i:i+batch_size]
            
            try:
                r = self.session.post(url, json=batch, timeout=120)
                
                if r.status_code in (200, 201):
                    stats['saved'] += len(batch)
                else:
                    stats['errors'] += len(batch)
            
            except Exception as e:
                print(f"  ⚠️ Erro ao salvar histórico: {e}")
                stats['errors'] += len(batch)
            
            time.sleep(0.3)
        
        return stats
    
    def test(self) -> bool:
        """Testa conexão"""
        try:
            url = f"{self.url}/rest/v1/"
            r = self.session.get(url, timeout=10)
            
            if r.status_code == 200:
                print("✅ Conexão com Supabase OK")
                return True
            else:
                print(f"❌ Erro HTTP {r.status_code}")
                return False
        except Exception as e:
            print(f"❌ Erro: {e}")
            return False
    
    def get_stats(self, tabela: str) -> dict:
        """Retorna estatísticas"""
        try:
            url = f"{self.url}/rest/v1/{tabela}"
            r = self.session.get(
                url,
                params={'select': 'count'},
                headers={**self.headers, 'Prefer': 'count=exact'},
                timeout=30
            )
            
            if r.status_code == 200:
                total = int(r.headers.get('Content-Range', '0').split('/')[-1])
                return {'total': total, 'table': tabela}
        except:
            pass
        
        return {'total': 0, 'table': tabela}
    
    def __del__(self):
        if hasattr(self, 'session'):
            self.session.close()


# ========== TESTE ==========
if __name__ == "__main__":
    print("\n🧪 TESTANDO SUPABASE CLIENT - ESTRUTURA SIMPLIFICADA\n")
    print("="*80)
    
    # Teste de preparação de item
    client = SupabaseClient()
    
    test_item = {
        'source': 'superbid',
        'external_id': 'superbid_123456',
        'title': 'Honda Civic 2020',
        'value': 50000,
        'value_text': 'R$ 50.000,00',
        'city': 'São Paulo',
        'state': 'SP',
        'auction_date': '2026-01-27 14:00:00-03',
        'has_bid': True,  # ✅ Boolean
        'auction_round': None,  # ✅ NULL = 1ª praça
        'vehicle_type': 'carro',
    }
    
    prepared = client._prepare(test_item, 'veiculos')
    
    print("Item preparado:")
    print(f"  has_bid: {prepared.get('has_bid')} (tipo: {type(prepared.get('has_bid'))})")
    print(f"  auction_round: {prepared.get('auction_round')} (tipo: {type(prepared.get('auction_round'))})")
    
    print("\n✅ Campos removidos (não presentes):")
    removed = ['total_bids', 'total_bidders', 'total_visits', 'days_remaining']
    for field in removed:
        if field not in prepared:
            print(f"  ❌ {field}: (removido corretamente)")
    
    print("\n✅ Campos mantidos:")
    kept = ['has_bid', 'auction_round']
    for field in kept:
        if field in prepared:
            print(f"  ✓ {field}: {prepared[field]}")
    
    print("="*80)